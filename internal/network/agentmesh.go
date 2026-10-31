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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// AgentMeshHost is the virtual hostname for private Agent Mesh URLs routed
// through the local agentmesh-node Unix socket.
const AgentMeshHost = "agentmesh.local"

// Headers injected by agentmesh-node after stripping any untrusted X-Mesh-*
// headers from the wire.
const (
	headerMeshIngress   = "X-Mesh-Ingress"
	headerMeshPrincipal = "X-Mesh-Principal"
	headerPeerID        = "X-Peer-ID"
)

// AgentMesh connects an agent server to an Agent Mesh network through a
// companion agentmesh-node process. Private traffic flows end-to-end over
// authenticated libp2p streams; when Public is enabled, the service is also
// exposed on the mesh routers' external HTTP ingress (/ingress/<peer_id>/a2a/<name>).
type AgentMesh struct {
	AgentName   string
	Description string
	StateDir    string
	SocketPath  string
	HubURL      string
	JoinToken   string
	NodeBin     string
	// Private opens the private mesh listener.
	Private bool
	// Public exposes the service via Agent Mesh external HTTP ingress.
	Public bool
	// IngressAuthMode is "passthrough" (default, preserving a2a pairing tokens)
	// or "edge" (Agent Mesh Biscuit/OIDC edge verification).
	IngressAuthMode string
	// IngressAllow restricts principals or roles when IngressAuthMode is "edge".
	IngressAllow []string
	Logf         func(string, ...any)

	mu            sync.Mutex
	started       bool
	bootstrapping atomic.Bool
	privateLn     net.Listener
	publicLn   net.Listener
	muxLn      net.Listener
	muxSrv     *http.Server
	cmd        *exec.Cmd
	cmdDone    chan error
	logFile    *os.File
	tokenFile  string
	peerID     string
	privateURL *url.URL
	publicURL  *url.URL

	whoIs sync.Map // local TCP addr ("127.0.0.1:port") -> whoIsEntry
}

type whoIsEntry struct {
	id   string
	edge bool
}

var _ Provider = (*AgentMesh)(nil)

// Name implements [Provider].
func (a *AgentMesh) Name() string { return "agentmesh" }

// ListenPrivate implements [Provider].
func (a *AgentMesh) ListenPrivate(ctx context.Context) (net.Listener, *url.URL, error) {
	a.mu.Lock()
	a.Private = true
	a.mu.Unlock()
	if err := a.start(ctx); err != nil {
		return nil, nil, err
	}
	return a.privateLn, a.privateURL, nil
}

// ListenPublic implements [Provider].
func (a *AgentMesh) ListenPublic(ctx context.Context) (net.Listener, *url.URL, error) {
	a.mu.Lock()
	a.Public = true
	a.mu.Unlock()
	if err := a.start(ctx); err != nil {
		return nil, nil, err
	}
	if a.publicLn == nil || a.publicURL == nil {
		return nil, nil, errors.New("agentmesh public ingress is not enabled")
	}
	return a.publicLn, a.publicURL, nil
}

type callerIDKey struct{}

// start opens the local listeners, writes agentmesh-node.yaml, launches the
// companion agentmesh-node process and waits until it joins the mesh.
func (a *AgentMesh) start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return nil
	}

	if err := os.MkdirAll(a.StateDir, 0o700); err != nil {
		return fmt.Errorf("creating agentmesh state directory: %w", err)
	}
	if a.SocketPath == "" {
		a.SocketPath = filepath.Join(a.StateDir, "agentmesh-node.sock")
	}

	var cleanups []func()
	rollback := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	var privateAddr, publicAddr string
	if a.Private {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("opening private loopback listener: %w", err)
		}
		a.privateLn = ln
		privateAddr = ln.Addr().String()
		cleanups = append(cleanups, func() { _ = ln.Close(); a.privateLn = nil })
	}
	if a.Public {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			rollback()
			return fmt.Errorf("opening public loopback listener: %w", err)
		}
		a.publicLn = ln
		publicAddr = ln.Addr().String()
		cleanups = append(cleanups, func() { _ = ln.Close(); a.publicLn = nil })
	}

	muxLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		rollback()
		return fmt.Errorf("opening agentmesh demux listener: %w", err)
	}
	a.muxLn = muxLn
	muxAddr := muxLn.Addr().String()
	muxSrv := a.newMuxServer(muxAddr, privateAddr, publicAddr)
	a.muxSrv = muxSrv
	go func() {
		_ = muxSrv.Serve(muxLn)
	}()
	cleanups = append(cleanups, func() {
		_ = muxSrv.Close()
		_ = muxLn.Close()
		a.muxSrv, a.muxLn = nil, nil
	})

	cfgPath := filepath.Join(a.StateDir, "agentmesh-node.yaml")
	if err := a.writeNodeConfig(cfgPath, "http://"+muxAddr); err != nil {
		rollback()
		return err
	}

	bin, err := a.resolveBinary()
	if err != nil {
		rollback()
		return err
	}

	args, tokenFile, err := a.buildNodeArgs(cfgPath)
	if err != nil {
		rollback()
		return err
	}
	a.tokenFile = tokenFile
	if tokenFile != "" {
		cleanups = append(cleanups, func() { _ = os.Remove(tokenFile); a.tokenFile = "" })
	}

	logPath := filepath.Join(a.StateDir, "agentmesh-node.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		rollback()
		return fmt.Errorf("opening %s: %w", logPath, err)
	}
	a.logFile = logFile
	cleanups = append(cleanups, func() { _ = logFile.Close(); a.logFile = nil })

	tail := &tailBuffer{max: 8 << 10}
	out := io.MultiWriter(logFile, tail)

	// While agentmesh-node initializes inside start(), a2a server has not yet
	// received privateLn/publicLn to start http.Serve, so answer the node's
	// initial local health probe with a minimal card so DHT + ingress publish
	// immediately without waiting on probe timeout.
	a.bootstrapping.Store(true)
	defer a.bootstrapping.Store(false)

	// Bound node lifetime to Provider.Close, not the setup call's ctx.
	cmd := exec.Command(bin, args...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		rollback()
		return fmt.Errorf("starting %s: %w", bin, err)
	}
	a.cmd = cmd
	a.cmdDone = make(chan error, 1)
	go func() {
		a.cmdDone <- cmd.Wait()
		close(a.cmdDone)
	}()
	cleanups = append(cleanups, func() { a.stopProcess() })

	info, err := a.waitReady(ctx, tail)
	if err != nil {
		rollback()
		return err
	}
	a.peerID = info.PeerID

	if a.Private {
		u, err := url.Parse(fmt.Sprintf("http://%s/mesh/%s/a2a/%s", AgentMeshHost, info.PeerID, a.AgentName))
		if err != nil {
			rollback()
			return err
		}
		a.privateURL = u
	}
	if a.Public {
		base, err := a.resolveIngressBase(ctx, info.IngressURLs)
		if err != nil {
			rollback()
			return err
		}
		u, err := url.Parse(fmt.Sprintf("%s/%s/a2a/%s", strings.TrimRight(base, "/"), info.PeerID, a.AgentName))
		if err != nil {
			rollback()
			return err
		}
		a.publicURL = u
	}

	a.started = true
	return nil
}

// newMuxServer builds the local HTTP reverse proxy that agentmesh-node forwards
// service requests to. It splits external ingress traffic from internal mesh
// traffic using the node-attested X-Mesh-Ingress header, records verified mesh
// principals per loopback connection for WhoIs, and normalizes AgentCard URLs
// back to muxAddr so agentmesh-node and agentmesh-router rewrite them cleanly.
func (a *AgentMesh) newMuxServer(muxAddr, privateAddr, publicAddr string) *http.Server {
	var d net.Dialer
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if entry, ok := ctx.Value(callerIDKey{}).(whoIsEntry); ok && entry.id != "" {
				local := conn.LocalAddr().String()
				a.whoIs.Store(local, entry)
				return &trackedConn{Conn: conn, onClose: func() { a.whoIs.Delete(local) }}, nil
			}
			return conn, nil
		},
	}

	targetBase := "http://" + muxAddr
	proxy := &httputil.ReverseProxy{
		FlushInterval: -1,
		Transport:     transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			dst, _ := pr.In.Context().Value(muxTargetKey{}).(string)
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = dst
			pr.Out.Host = dst
			// Ask uncompressed cards so ModifyResponse can normalize interface URLs.
			if isCardPath(pr.Out.URL.Path) {
				pr.Out.Header.Del("Accept-Encoding")
			}
		},
		ModifyResponse: func(resp *http.Response) error {
			if resp.Request == nil || resp.Request.Method != http.MethodGet ||
				!isCardPath(resp.Request.URL.Path) || resp.StatusCode != http.StatusOK {
				return nil
			}
			return a.normalizeCardResponse(resp, targetBase)
		},
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := strings.TrimSpace(r.Header.Get(headerMeshIngress))
		principal := strings.TrimSpace(r.Header.Get(headerMeshPrincipal))
		peerID := strings.TrimSpace(r.Header.Get(headerPeerID))

		if a.bootstrapping.Load() && mode == "" && peerID == "" && r.Method == http.MethodGet && isCardPath(r.URL.Path) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":        a.AgentName,
				"description": a.Description,
				"supportedInterfaces": []map[string]string{{
					"url":             targetBase,
					"protocolBinding": "JSONRPC",
					"protocolVersion": "1.0",
				}},
			})
			return
		}

		var dst string
		var caller whoIsEntry
		switch mode {
		case "passthrough":
			if publicAddr == "" {
				http.NotFound(w, r)
				return
			}
			dst = publicAddr
		case "edge":
			if principal == "" {
				// Unauthenticated AgentCard discovery over edge ingress.
				dst = publicAddr
				if dst == "" {
					dst = privateAddr
				}
			} else if privateAddr != "" {
				dst = privateAddr
				caller = whoIsEntry{id: principal, edge: true}
			} else {
				dst = publicAddr
			}
			if dst == "" {
				http.NotFound(w, r)
				return
			}
		case "":
			if privateAddr != "" {
				dst = privateAddr
				id := principal
				if id == "" {
					id = peerID
				}
				caller = whoIsEntry{id: id}
			} else if publicAddr != "" && peerID == "" && r.Method == http.MethodGet && isCardPath(r.URL.Path) {
				// Allow local A2AService.Probe when only public ingress is configured.
				dst = publicAddr
			} else {
				http.NotFound(w, r)
				return
			}
		default:
			http.Error(w, "unsupported ingress mode", http.StatusForbidden)
			return
		}

		ctx := context.WithValue(r.Context(), muxTargetKey{}, dst)
		if caller.id != "" {
			ctx = context.WithValue(ctx, callerIDKey{}, caller)
		}
		proxy.ServeHTTP(w, r.WithContext(ctx))
	})

	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 30 * time.Second,
	}
}

type muxTargetKey struct{}

func isCardPath(p string) bool {
	return p == "/" || p == "/.well-known/agent-card.json" || p == "/.well-known/agent.json"
}

func (a *AgentMesh) normalizeCardResponse(resp *http.Response, targetBase string) error {
	var prefixes []string
	a.mu.Lock()
	if a.privateURL != nil {
		prefixes = append(prefixes, strings.TrimRight(a.privateURL.String(), "/"))
	}
	if a.publicURL != nil {
		prefixes = append(prefixes, strings.TrimRight(a.publicURL.String(), "/"))
	}
	a.mu.Unlock()
	if len(prefixes) == 0 {
		return nil
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	_ = resp.Body.Close()

	out := rewriteCardInterfaces(raw, targetBase, prefixes)
	resp.Body = io.NopCloser(bytes.NewReader(out))
	resp.ContentLength = int64(len(out))
	resp.Header.Set("Content-Length", strconv.Itoa(len(out)))
	return nil
}

// rewriteCardInterfaces replaces private/public listener base URLs in
// supportedInterfaces[].url with targetBase so agentmesh-node sees URLs
// relative to its configured target_url.
func rewriteCardInterfaces(raw []byte, targetBase string, prefixes []string) []byte {
	var card map[string]json.RawMessage
	if err := json.Unmarshal(raw, &card); err != nil {
		return raw
	}
	ifacesRaw, ok := card["supportedInterfaces"]
	if !ok {
		return raw
	}
	var ifaces []map[string]json.RawMessage
	if err := json.Unmarshal(ifacesRaw, &ifaces); err != nil {
		return raw
	}
	changed := false
	for _, iface := range ifaces {
		var u string
		if iface == nil || json.Unmarshal(iface["url"], &u) != nil {
			continue
		}
		for _, pfx := range prefixes {
			if pfx == "" {
				continue
			}
			var next string
			switch {
			case u == pfx || u == pfx+"/":
				next = targetBase
			case strings.HasPrefix(u, pfx+"/"):
				next = targetBase + strings.TrimPrefix(u, pfx)
			}
			if next != "" {
				if enc, err := json.Marshal(next); err == nil {
					iface["url"] = enc
					changed = true
				}
				break
			}
		}
	}
	if !changed {
		return raw
	}
	enc, err := json.Marshal(ifaces)
	if err != nil {
		return raw
	}
	card["supportedInterfaces"] = enc
	out, err := json.Marshal(card)
	if err != nil {
		return raw
	}
	return out
}

type nodeConfigYAML struct {
	Version  string              `yaml:"version"`
	Services []nodeServiceConfig `yaml:"services"`
}

type nodeServiceConfig struct {
	Name        string             `yaml:"name"`
	Type        string             `yaml:"type"`
	Description string             `yaml:"description,omitempty"`
	TargetURL   string             `yaml:"target_url"`
	Ingress     *nodeIngressConfig `yaml:"ingress,omitempty"`
}

type nodeIngressConfig struct {
	AuthMode string   `yaml:"auth_mode,omitempty"`
	Allow    []string `yaml:"allow,omitempty"`
}

func (a *AgentMesh) writeNodeConfig(path, targetURL string) error {
	svc := nodeServiceConfig{
		Name:        a.AgentName,
		Type:        "a2a",
		Description: a.Description,
		TargetURL:   targetURL,
	}
	if a.Public {
		mode := a.IngressAuthMode
		if mode == "" {
			mode = "passthrough"
		}
		svc.Ingress = &nodeIngressConfig{
			AuthMode: mode,
			Allow:    a.IngressAllow,
		}
	}
	data, err := yaml.Marshal(nodeConfigYAML{
		Version:  "v1alpha1",
		Services: []nodeServiceConfig{svc},
	})
	if err != nil {
		return fmt.Errorf("marshaling agentmesh-node config: %w", err)
	}
	return os.WriteFile(path, data, 0o600)
}

func (a *AgentMesh) resolveBinary() (string, error) {
	if a.NodeBin != "" {
		return a.NodeBin, nil
	}
	if env := strings.TrimSpace(os.Getenv("AGENTMESH_NODE_BIN")); env != "" {
		return env, nil
	}
	bin, err := exec.LookPath("agentmesh-node")
	if err != nil {
		return "", errors.New("agentmesh-node binary not found on PATH (install agentmesh-node or set AGENTMESH_NODE_BIN)")
	}
	return bin, nil
}

func (a *AgentMesh) buildNodeArgs(cfgPath string) ([]string, string, error) {
	hub := strings.TrimSpace(a.HubURL)
	if hub == "" {
		hub = strings.TrimSpace(os.Getenv("AGENTMESH_HUB"))
	}
	hasIdentity := fileExists(filepath.Join(a.StateDir, "credential.pb"))
	joinToken := strings.TrimSpace(a.JoinToken)
	if joinToken == "" {
		joinToken = strings.TrimSpace(os.Getenv("AGENTMESH_JOIN_TOKEN"))
	}

	if !hasIdentity && joinToken == "" {
		if hub == "" {
			return nil, "", fmt.Errorf("agentmesh is not enrolled yet: pass --agentmesh-hub <url> and --agentmesh-token env:AGENTMESH_JOIN_TOKEN, or run `agentmesh-node --data-dir %s join <hub-url>`", a.StateDir)
		}
		return nil, "", fmt.Errorf("agentmesh has no stored identity in %s: pass --agentmesh-token env:AGENTMESH_JOIN_TOKEN, or enroll once with `agentmesh-node --data-dir %s join %s`", a.StateDir, a.StateDir, hub)
	}
	if joinToken != "" && hub == "" && !hasIdentity {
		return nil, "", errors.New("agentmesh needs --agentmesh-hub <url> (or $AGENTMESH_HUB) to enroll with a join token")
	}

	args := []string{
		"run",
		"--data-dir", a.StateDir,
		"--config", cfgPath,
		"--bind-addr", "",
		"--socket-path", a.SocketPath,
		"--listen", "/ip4/0.0.0.0/udp/0/quic-v1",
		"--listen", "/ip4/0.0.0.0/tcp/0",
		"--discovery-interval", "500ms",
	}
	if hub != "" {
		args = append(args, "--control-plane", hub)
		if isLoopbackURL(hub) {
			args = append(args, "--allow-loopback")
		}
	}

	var tokenFile string
	if joinToken != "" && hub != "" {
		tokenFile = filepath.Join(a.StateDir, "bootstrap.token")
		if err := os.WriteFile(tokenFile, []byte(joinToken+"\n"), 0o600); err != nil {
			return nil, "", fmt.Errorf("writing bootstrap token file: %w", err)
		}
		args = append(args, "--bootstrap-token-path", tokenFile)
	}
	return args, tokenFile, nil
}

type nodeMeshInfo struct {
	PeerID      string   `json:"peer_id"`
	IngressURLs []string `json:"ingress_urls"`
}

func (a *AgentMesh) waitReady(ctx context.Context, tail *tailBuffer) (nodeMeshInfo, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	hc := a.socketClient(3 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case err := <-a.cmdDone:
			return nodeMeshInfo{}, fmt.Errorf("agentmesh-node exited before ready (%v):\n%s", err, strings.TrimSpace(tail.String()))
		case <-waitCtx.Done():
			return nodeMeshInfo{}, fmt.Errorf("waiting for agentmesh-node to join the mesh: %w\n%s", waitCtx.Err(), strings.TrimSpace(tail.String()))
		case <-ticker.C:
			req, err := http.NewRequestWithContext(waitCtx, http.MethodGet, "http://"+AgentMeshHost+"/readyz", nil)
			if err != nil {
				continue
			}
			resp, err := hc.Do(req)
			if err != nil {
				continue
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				continue
			}
			infoReq, err := http.NewRequestWithContext(waitCtx, http.MethodGet, "http://"+AgentMeshHost+"/debug/mesh-info", nil)
			if err != nil {
				continue
			}
			infoResp, err := hc.Do(infoReq)
			if err != nil {
				continue
			}
			var info nodeMeshInfo
			decErr := json.NewDecoder(io.LimitReader(infoResp.Body, 1<<20)).Decode(&info)
			_ = infoResp.Body.Close()
			if decErr == nil && infoResp.StatusCode == http.StatusOK && info.PeerID != "" {
				return info, nil
			}
		}
	}
}

func (a *AgentMesh) resolveIngressBase(ctx context.Context, ingressURLs []string) (string, error) {
	for _, u := range ingressURLs {
		if strings.TrimSpace(u) != "" {
			return strings.TrimRight(strings.TrimSpace(u), "/"), nil
		}
	}
	hub := strings.TrimSpace(a.HubURL)
	if hub == "" {
		hub = strings.TrimSpace(os.Getenv("AGENTMESH_HUB"))
	}
	if hub == "" {
		return "", errors.New("mesh routers advertise no ingress_url and no --agentmesh-hub URL is set")
	}
	// agentmesh-one serves external HTTP ingress under <hub>/ingress.
	return strings.TrimRight(hub, "/") + "/ingress", nil
}

// agentMeshCapKey matches auth.TailscaleCap so edge-verified Agent Mesh
// callers receive member access unless overridden in network.identities.
const agentMeshCapKey = "a2aproject.org/cap/a2a"

// WhoIs implements [Provider].
func (a *AgentMesh) WhoIs(_ context.Context, remoteAddr string) (*Peer, error) {
	v, ok := a.whoIs.Load(remoteAddr)
	if !ok {
		return nil, ErrNoIdentity
	}
	entry, _ := v.(whoIsEntry)
	if entry.id == "" {
		return nil, ErrNoIdentity
	}
	peer := &Peer{ID: entry.id}
	if entry.edge {
		peer.Caps = map[string][]json.RawMessage{
			agentMeshCapKey: {json.RawMessage(`{"role":"member"}`)},
		}
	}
	return peer, nil
}

// Peers implements [Provider].
func (a *AgentMesh) Peers(ctx context.Context) ([]PeerInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	hc := a.HTTPClient()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+AgentMeshHost+"/mesh/service/discover?type=a2a", nil)
	if err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("discovering a2a services on agentmesh: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("agentmesh service discovery (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var found []struct {
		PeerID         string `json:"peer_id"`
		SrvName        string `json:"srv_name"`
		SrvDescription string `json:"srv_description"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&found); err != nil {
		return nil, fmt.Errorf("decoding agentmesh discovery response: %w", err)
	}

	a.mu.Lock()
	selfPeer := a.peerID
	a.mu.Unlock()

	candidates := make([]PeerInfo, 0, len(found))
	for _, p := range found {
		if p.PeerID == "" || p.SrvName == "" || p.PeerID == selfPeer {
			continue
		}
		candidates = append(candidates, PeerInfo{
			Name:        p.SrvName,
			URL:         fmt.Sprintf("http://%s/mesh/%s/a2a/%s", AgentMeshHost, p.PeerID, p.SrvName),
			Description: p.SrvDescription,
			Online:      true,
		})
	}
	return ProbeAgents(ctx, hc, candidates), nil
}

// HTTPClient implements [Provider]. Requests to http://agentmesh.local/...
// travel over the local agentmesh-node Unix socket; all other URLs use
// standard HTTP transport.
func (a *AgentMesh) HTTPClient() *http.Client {
	return a.socketClient(0)
}

func (a *AgentMesh) socketClient(timeout time.Duration) *http.Client {
	var d net.Dialer
	socketTr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return d.DialContext(ctx, "unix", a.SocketPath)
		},
	}
	return &http.Client{
		Timeout: timeout,
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if strings.EqualFold(r.URL.Host, AgentMeshHost) {
				return socketTr.RoundTrip(r)
			}
			return http.DefaultTransport.RoundTrip(r)
		}),
	}
}

// Close implements [Provider].
func (a *AgentMesh) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.stopProcess()
	var errs []error
	if a.muxSrv != nil {
		errs = append(errs, a.muxSrv.Close())
		a.muxSrv = nil
	}
	if a.muxLn != nil {
		if err := a.muxLn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
		a.muxLn = nil
	}
	if a.privateLn != nil {
		if err := a.privateLn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
		a.privateLn = nil
	}
	if a.publicLn != nil {
		if err := a.publicLn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
		a.publicLn = nil
	}
	if a.logFile != nil {
		errs = append(errs, a.logFile.Close())
		a.logFile = nil
	}
	if a.tokenFile != "" {
		_ = os.Remove(a.tokenFile)
		a.tokenFile = ""
	}
	if a.SocketPath != "" {
		_ = os.Remove(a.SocketPath)
	}
	a.started = false
	return errors.Join(errs...)
}

func (a *AgentMesh) stopProcess() {
	if a.cmd == nil || a.cmd.Process == nil {
		return
	}
	_ = a.cmd.Process.Signal(os.Interrupt)
	select {
	case <-a.cmdDone:
	case <-time.After(5 * time.Second):
		_ = a.cmd.Process.Kill()
		<-a.cmdDone
	}
	a.cmd = nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type trackedConn struct {
	net.Conn
	once    sync.Once
	onClose func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.onClose)
	return err
}

type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.max {
		b.buf = append([]byte(nil), b.buf[len(b.buf)-b.max:]...)
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func isLoopbackURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
