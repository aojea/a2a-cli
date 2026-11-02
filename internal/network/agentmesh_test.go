// Copyright 2026 The A2A Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package network

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/agentmesh/api"
	"github.com/google/agentmesh/sdk/meshnet"
	"github.com/google/go-cmp/cmp"
)

type fakeMeshSession struct {
	mu        sync.Mutex
	callers   map[string]*meshnet.CallerInfo
	providers []*api.DiscoveredProvider
	hc        *http.Client
	lastTAR   *api.TaskAuthorizationRule
	lastSeal  bool
}

func (f *fakeMeshSession) WhoIs(addr string) (*meshnet.CallerInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.callers[addr]
	return c, ok
}

func (f *fakeMeshSession) Discover(_ context.Context, _ api.ServiceType, _ string) ([]*api.DiscoveredProvider, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.providers, nil
}

func (f *fakeMeshSession) HTTPClient() *http.Client {
	if f.hc != nil {
		return f.hc
	}
	return http.DefaultClient
}

func (f *fakeMeshSession) AttenuateToken(rule *api.TaskAuthorizationRule, _ time.Duration, seal bool) (string, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastTAR = rule
	f.lastSeal = seal
	return "attenuated-biscuit-token", time.Now().Add(10 * time.Minute), nil
}

func (f *fakeMeshSession) Close() error { return nil }

type pipeSourceListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func newPipeSourceListener() *pipeSourceListener {
	return &pipeSourceListener{
		conns: make(chan net.Conn, 8),
		done:  make(chan struct{}),
	}
}

func (l *pipeSourceListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeSourceListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *pipeSourceListener) Addr() net.Addr { return fakeAddr("meshnet:test") }

type fakeAddr string

func (a fakeAddr) Network() string { return "meshnet" }
func (a fakeAddr) String() string  { return string(a) }

type remoteNamedConn struct {
	net.Conn
	remote string
}

func (c *remoteNamedConn) RemoteAddr() net.Addr { return fakeAddr(c.remote) }

func TestAgentMeshSplitAndWhoIs(t *testing.T) {
	t.Parallel()

	sess := &fakeMeshSession{
		callers: map[string]*meshnet.CallerInfo{
			"meshnet:internal#1": {
				PeerID:    "12D3KooWCaller",
				Principal: "email:alice@example.com",
			},
			"meshnet:edge#2": {
				PeerID:    "12D3KooWBob",
				Principal: "email:bob@example.com",
			},
		},
	}

	privSrc := newPipeSourceListener()
	pubSrc := newPipeSourceListener()
	privURL, err := url.Parse("http://agentmesh.local/mesh/12D3KooWSelf/a2a/coder")
	if err != nil {
		t.Fatal(err)
	}
	pubURL, err := url.Parse("https://hub.example.com/ingress/12D3KooWSelf/a2a/coder")
	if err != nil {
		t.Fatal(err)
	}

	am := &AgentMesh{
		AgentName: "coder",
		Private:   true,
		Public:    true,
		srv:       sess,
		peerID:    "12D3KooWSelf",
	}
	am.attachEndpoints(privSrc, privURL, pubSrc, pubURL)
	am.started.Store(true)
	t.Cleanup(func() {
		_ = privSrc.Close()
		_ = pubSrc.Close()
		_ = am.Close()
	})

	serveListener := func(ln net.Listener, name string) *http.Server {
		srv := &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				peer, wErr := am.WhoIs(r.Context(), r.RemoteAddr)
				id := ""
				hasEdgeCap := false
				if wErr == nil {
					id = peer.ID
					hasEdgeCap = len(peer.Caps[agentMeshCapKey]) > 0
				} else if !errors.Is(wErr, ErrNoIdentity) {
					t.Errorf("WhoIs(%q) unexpected error = %v", r.RemoteAddr, wErr)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"listener": name,
					"whois":    id,
					"edgeCap":  hasEdgeCap,
					"auth":     r.Header.Get("Authorization"),
				})
			}),
		}
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(func() { _ = srv.Close() })
		return srv
	}
	serveListener(am.private, "private")
	serveListener(am.public, "public")

	doOverPipe := func(t *testing.T, src *pipeSourceListener, remoteKey, authHdr string) map[string]any {
		t.Helper()
		clientSide, serverSide := net.Pipe()
		src.conns <- &remoteNamedConn{Conn: serverSide, remote: remoteKey}
		var once sync.Once
		hc := &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
				DialContext: func(context.Context, string, string) (net.Conn, error) {
					var c net.Conn
					once.Do(func() { c = clientSide })
					if c == nil {
						return nil, errors.New("pipe already used")
					}
					return c, nil
				},
			},
		}
		req, rErr := http.NewRequest(http.MethodPost, "http://agentmesh.local/", strings.NewReader("{}"))
		if rErr != nil {
			t.Fatal(rErr)
		}
		if authHdr != "" {
			req.Header.Set("Authorization", authHdr)
		}
		resp, dErr := hc.Do(req)
		if dErr != nil {
			t.Fatalf("hc.Do(%s): %v", remoteKey, dErr)
		}
		defer func() { _ = resp.Body.Close() }()
		var got map[string]any
		if jErr := json.NewDecoder(resp.Body).Decode(&got); jErr != nil {
			t.Fatal(jErr)
		}
		return got
	}

	t.Run("internal mesh stream routes to private listener without edge cap", func(t *testing.T) {
		got := doOverPipe(t, privSrc, "meshnet:internal#1", "Bearer a2p_mesh")
		want := map[string]any{
			"listener": "private",
			"whois":    "email:alice@example.com",
			"edgeCap":  false,
			"auth":     "Bearer a2p_mesh",
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("private stream mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("passthrough external ingress routes to public listener with ErrNoIdentity", func(t *testing.T) {
		got := doOverPipe(t, pubSrc, "meshnet:unverified#1", "Bearer a2p_external")
		want := map[string]any{
			"listener": "public",
			"whois":    "",
			"edgeCap":  false,
			"auth":     "Bearer a2p_external",
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("passthrough stream mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("edge-verified external ingress routes to private listener with member capability", func(t *testing.T) {
		got := doOverPipe(t, pubSrc, "meshnet:edge#2", "")
		want := map[string]any{
			"listener": "private",
			"whois":    "email:bob@example.com",
			"edgeCap":  true,
			"auth":     "",
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("edge stream mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestAgentMeshPeersAndHTTPClient(t *testing.T) {
	t.Parallel()

	meshPeerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mesh/12D3KooWRemote/a2a/reviewer/.well-known/agent-card.json" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"name":        "reviewer",
			"description": "Reviews pull requests.",
		})
	}))
	defer meshPeerSrv.Close()

	extSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "external-ok")
	}))
	defer extSrv.Close()

	meshHC := &http.Client{
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			u, err := url.Parse(meshPeerSrv.URL + r.URL.Path)
			if err != nil {
				return nil, err
			}
			out := r.Clone(r.Context())
			out.URL = u
			out.Host = u.Host
			return http.DefaultTransport.RoundTrip(out)
		}),
	}

	sess := &fakeMeshSession{
		hc: meshHC,
		providers: []*api.DiscoveredProvider{
			{PeerId: "12D3KooWSelf", SrvName: "coder", SrvDescription: "self"},
			{PeerId: "12D3KooWRemote", SrvName: "reviewer", SrvDescription: "code reviewer"},
		},
	}

	am := &AgentMesh{
		AgentName: "coder",
		srv:       sess,
		peerID:    "12D3KooWSelf",
	}
	am.started.Store(true)

	peers, err := am.Peers(t.Context())
	if err != nil {
		t.Fatalf("Peers() error = %v", err)
	}
	want := []PeerInfo{{
		Name:        "reviewer",
		URL:         "http://agentmesh.local/mesh/12D3KooWRemote/a2a/reviewer",
		Description: "Reviews pull requests.",
		Online:      true,
	}}
	if diff := cmp.Diff(want, peers); diff != "" {
		t.Fatalf("Peers() mismatch (-want +got):\n%s", diff)
	}

	// Non-mesh URLs fall back to http.DefaultTransport.
	resp, err := am.HTTPClient().Get(extSrv.URL)
	if err != nil {
		t.Fatalf("HTTPClient().Get(external) error = %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "external-ok" {
		t.Fatalf("external body = %q, want %q", body, "external-ok")
	}
}

func TestAgentMeshAttenuateAndValidation(t *testing.T) {
	t.Parallel()

	sess := &fakeMeshSession{}
	am := &AgentMesh{srv: sess}
	am.started.Store(true)

	if _, _, err := am.AttenuateToken(nil, "", 0); err == nil {
		t.Fatal("AttenuateToken(empty) error = nil, want error")
	}
	tok, _, err := am.AttenuateToken([]string{"a2a://auditor"}, "method:POST", 5*time.Minute)
	if err != nil {
		t.Fatalf("AttenuateToken() error = %v", err)
	}
	if tok != "attenuated-biscuit-token" || !sess.lastSeal {
		t.Fatalf("AttenuateToken() = %q (seal=%v), want sealed token", tok, sess.lastSeal)
	}
	if len(sess.lastTAR.GetRules()) != 1 || len(sess.lastTAR.GetRules()[0].GetAllowedServices()) != 1 ||
		sess.lastTAR.GetRules()[0].GetAllowedServices()[0] != "a2a://auditor" {
		t.Fatalf("TAR rules = %+v, want allowed_services=[a2a://auditor]", sess.lastTAR.GetRules())
	}

	unstarted := &AgentMesh{StateDir: t.TempDir()}
	if _, _, _, err := unstarted.resolveEnrollment(); err == nil {
		t.Fatal("resolveEnrollment() with empty StateDir and no token = nil, want error")
	}
	withTokenNoHub := &AgentMesh{StateDir: t.TempDir(), JoinToken: "tok"}
	if _, _, _, err := withTokenNoHub.resolveEnrollment(); err == nil {
		t.Fatal("resolveEnrollment() with token and no hub = nil, want error")
	}
	if _, err := parseIngressAuthMode(true, "invalid"); err == nil {
		t.Fatal("parseIngressAuthMode(invalid) = nil, want error")
	}
}

func TestAgentMeshLiveStandaloneIntegration(t *testing.T) {
	oneBin := strings.TrimSpace(os.Getenv("AGENTMESH_ONE_BIN"))
	if oneBin == "" {
		if found, err := exec.LookPath("agentmesh-one"); err == nil {
			oneBin = found
		}
	}
	if oneBin == "" {
		t.Skip("AGENTMESH_ONE_BIN not set and agentmesh-one not on PATH")
	}

	hubDir := t.TempDir()
	policyJSON := `{
  "roles": [
    {"name": "mesh-admin", "allowed_services": ["*"], "allowed_targets": ["*"], "allowed_ingress": [{"services": ["*"]}]},
    {"name": "mesh:role:router", "allowed_services": ["*"], "allowed_targets": ["*"]},
    {
      "name": "mesh:role:node",
      "allowed_services": ["*"],
      "allowed_targets": ["*"],
      "allowed_publish": ["*"],
      "allowed_labels": ["*"],
      "allowed_ingress": [
        {"services": ["a2a://*"], "allowed_auth_modes": ["INGRESS_AUTH_MODE_EDGE", "INGRESS_AUTH_MODE_PASSTHROUGH"]}
      ]
    }
  ]
}`
	policyPath := filepath.Join(hubDir, "policy.json")
	if err := os.WriteFile(policyPath, []byte(policyJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	hubLogPath := filepath.Join(hubDir, "agentmesh-one.log")
	hubLogFile, err := os.Create(hubLogPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hubLogFile.Close() })
	cmd := exec.Command(oneBin,
		"--bind-address", "127.0.0.1",
		"--port", "0",
		"--data-dir", hubDir,
		"--policy-file", policyPath,
	)
	cmd.Stdout = hubLogFile
	cmd.Stderr = hubLogFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting agentmesh-one: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	joinTokenPath := filepath.Join(hubDir, "join-token")
	deadline := time.Now().Add(10 * time.Second)
	var hubURL, joinToken string
	for time.Now().Before(deadline) {
		if hubURL == "" {
			if rawLog, lErr := os.ReadFile(hubLogPath); lErr == nil {
				for _, line := range strings.Split(string(rawLog), "\n") {
					if after, ok := strings.CutPrefix(line, "API URL:"); ok {
						hubURL = strings.TrimSpace(after)
						break
					}
				}
			}
		}
		if hubURL != "" {
			resp, rErr := http.Get(hubURL + "/readyz")
			if rErr == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					if raw, fErr := os.ReadFile(joinTokenPath); fErr == nil && strings.TrimSpace(string(raw)) != "" {
						joinToken = strings.TrimSpace(string(raw))
						break
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if joinToken == "" {
		rawLog, _ := os.ReadFile(hubLogPath)
		t.Fatalf("timed out waiting for agentmesh-one /readyz and join-token; hub log:\n%s", rawLog)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	coder := &AgentMesh{
		AgentName:       "coder",
		Description:     "Coder agent",
		StateDir:        filepath.Join(t.TempDir(), "coder"),
		HubURL:          hubURL,
		JoinToken:       joinToken,
		Private:         true,
		Public:          true,
		IngressAuthMode: "passthrough",
	}
	t.Cleanup(func() { _ = coder.Close() })
	coderPrivLn, _, err := coder.ListenPrivate(ctx)
	if err != nil {
		t.Fatalf("coder.ListenPrivate: %v", err)
	}
	coderPubLn, coderPubURL, err := coder.ListenPublic(ctx)
	if err != nil {
		t.Fatalf("coder.ListenPublic: %v", err)
	}
	coderSrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/agent-card.json" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":        "coder",
				"description": "Coder agent",
				"supportedInterfaces": []map[string]string{{
					"url":             coderPubURL.String(),
					"protocolBinding": "JSONRPC",
				}},
			})
			return
		}
		_, _ = io.WriteString(w, "coder-ok:"+r.Header.Get("Authorization"))
	})}
	go func() { _ = coderSrv.Serve(coderPrivLn) }()
	go func() { _ = coderSrv.Serve(coderPubLn) }()
	t.Cleanup(func() { _ = coderSrv.Close() })

	auditor := &AgentMesh{
		AgentName:       "auditor",
		Description:     "Auditor agent",
		StateDir:        filepath.Join(t.TempDir(), "auditor"),
		HubURL:          hubURL,
		JoinToken:       joinToken,
		Private:         true,
		Public:          true,
		IngressAuthMode: "edge",
	}
	t.Cleanup(func() { _ = auditor.Close() })
	audPrivLn, _, err := auditor.ListenPrivate(ctx)
	if err != nil {
		t.Fatalf("auditor.ListenPrivate: %v", err)
	}
	audPubLn, audPubURL, err := auditor.ListenPublic(ctx)
	if err != nil {
		t.Fatalf("auditor.ListenPublic: %v", err)
	}
	audSrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/agent-card.json" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":        "auditor",
				"description": "Auditor agent",
				"supportedInterfaces": []map[string]string{{
					"url":             audPubURL.String(),
					"protocolBinding": "JSONRPC",
				}},
			})
			return
		}
		peer, wErr := auditor.WhoIs(r.Context(), r.RemoteAddr)
		if wErr != nil {
			http.Error(w, wErr.Error(), http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, "auditor-whois:"+peer.ID)
	})}
	go func() { _ = audSrv.Serve(audPrivLn) }()
	go func() { _ = audSrv.Serve(audPubLn) }()
	t.Cleanup(func() { _ = audSrv.Close() })

	// 1. DHT discovery + private P2P call from coder -> auditor.
	var peers []PeerInfo
	for range 20 {
		peers, err = coder.Peers(ctx)
		if err == nil && len(peers) == 1 && peers[0].Name == "auditor" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(peers) != 1 || peers[0].Name != "auditor" {
		t.Fatalf("coder.Peers() = %+v (err=%v), want [auditor]", peers, err)
	}

	// 2. Passthrough public ingress preserves Authorization header.
	pubReq, err := http.NewRequestWithContext(ctx, http.MethodPost, coderPubURL.String()+"/invoke", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	pubReq.Header.Set("Authorization", "Bearer a2p_pair123")
	pubResp, err := http.DefaultClient.Do(pubReq)
	if err != nil {
		t.Fatalf("passthrough POST: %v", err)
	}
	pubBody, _ := io.ReadAll(pubResp.Body)
	_ = pubResp.Body.Close()
	if pubResp.StatusCode != http.StatusOK || string(pubBody) != "coder-ok:Bearer a2p_pair123" {
		t.Fatalf("passthrough status=%d body=%q", pubResp.StatusCode, pubBody)
	}

	// 3. Edge ingress rejects wrong-audience Biscuit (403) and admits right-audience Biscuit (200).
	wrongTok, _, err := coder.AttenuateToken([]string{"a2a://coder"}, "", 5*time.Minute)
	if err != nil {
		t.Fatalf("AttenuateToken(coder): %v", err)
	}
	wrongReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, audPubURL.String()+"/invoke", strings.NewReader("{}"))
	wrongReq.Header.Set("Authorization", "Bearer "+wrongTok)
	wrongResp, err := http.DefaultClient.Do(wrongReq)
	if err != nil {
		t.Fatalf("wrong-audience POST: %v", err)
	}
	_ = wrongResp.Body.Close()
	if wrongResp.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong-audience status = %d, want 403", wrongResp.StatusCode)
	}

	rightTok, _, err := coder.AttenuateToken([]string{"a2a://auditor"}, "", 5*time.Minute)
	if err != nil {
		t.Fatalf("AttenuateToken(auditor): %v", err)
	}
	rightReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, audPubURL.String()+"/invoke", strings.NewReader("{}"))
	rightReq.Header.Set("Authorization", "Bearer "+rightTok)
	rightResp, err := http.DefaultClient.Do(rightReq)
	if err != nil {
		t.Fatalf("right-audience POST: %v", err)
	}
	rightBody, _ := io.ReadAll(rightResp.Body)
	_ = rightResp.Body.Close()
	if rightResp.StatusCode != http.StatusOK || !strings.HasPrefix(string(rightBody), "auditor-whois:") {
		t.Fatalf("right-audience status=%d body=%q, want 200 auditor-whois:...", rightResp.StatusCode, rightBody)
	}
}
