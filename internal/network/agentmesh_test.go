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
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestAgentMeshMuxRoutingAndCardRewrite(t *testing.T) {
	t.Parallel()
	privURL, err := url.Parse("http://agentmesh.local/mesh/12D3KooWPeerA/a2a/coder")
	if err != nil {
		t.Fatal(err)
	}
	pubURL, err := url.Parse("https://hub.example.com/ingress/12D3KooWPeerA/a2a/coder")
	if err != nil {
		t.Fatal(err)
	}

	am := &AgentMesh{
		AgentName:  "coder",
		privateURL: privURL,
		publicURL:  pubURL,
	}

	privSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && isCardPath(r.URL.Path) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name": "coder",
				"supportedInterfaces": []map[string]any{
					{"url": privURL.String(), "protocolBinding": "JSONRPC"},
				},
			})
			return
		}
		peer, err := am.WhoIs(r.Context(), r.RemoteAddr)
		id := ""
		if err == nil {
			id = peer.ID
		} else if !errors.Is(err, ErrNoIdentity) {
			t.Errorf("WhoIs() unexpected error = %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"listener": "private",
			"whois":    id,
			"auth":     r.Header.Get("Authorization"),
		})
	}))
	defer privSrv.Close()

	pubSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && isCardPath(r.URL.Path) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name": "coder",
				"supportedInterfaces": []map[string]any{
					{"url": pubURL.String(), "protocolBinding": "JSONRPC"},
				},
				"securitySchemes": map[string]any{
					"pairing": map[string]any{"scheme": "Bearer"},
				},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"listener": "public",
			"auth":     r.Header.Get("Authorization"),
		})
	}))
	defer pubSrv.Close()

	muxLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = muxLn.Close() }()
	muxAddr := muxLn.Addr().String()
	muxSrv := am.newMuxServer(
		muxAddr,
		strings.TrimPrefix(privSrv.URL, "http://"),
		strings.TrimPrefix(pubSrv.URL, "http://"),
	)
	defer func() { _ = muxSrv.Close() }()
	go func() { _ = muxSrv.Serve(muxLn) }()

	base := "http://" + muxAddr

	t.Run("internal mesh call routes to private listener with WhoIs identity", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, base+"/", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(headerPeerID, "12D3KooWCaller")
		req.Header.Set(headerMeshPrincipal, "alice@example.com")
		req.Header.Set("Authorization", "Bearer a2p_mesh")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do() error = %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		var got map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		want := map[string]string{"listener": "private", "whois": "alice@example.com", "auth": "Bearer a2p_mesh"}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("private route mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("passthrough external ingress routes to public listener and keeps bearer token", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, base+"/pair", strings.NewReader(`{"code":"ABCD-EFGH"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(headerMeshIngress, "passthrough")
		req.Header.Set("Authorization", "Bearer a2p_external")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do() error = %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		var got map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		want := map[string]string{"listener": "public", "auth": "Bearer a2p_external"}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("passthrough route mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("edge external ingress with verified principal routes to private listener with WhoIs", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, base+"/", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(headerMeshIngress, "edge")
		req.Header.Set(headerMeshPrincipal, "bob@example.com")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do() error = %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		var got map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		want := map[string]string{"listener": "private", "whois": "bob@example.com", "auth": ""}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("edge route mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("public card normalizes interface URL back to mux target while keeping securitySchemes", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, base+"/.well-known/agent-card.json", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(headerMeshIngress, "passthrough")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do() error = %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		var card struct {
			SupportedInterfaces []struct {
				URL string `json:"url"`
			} `json:"supportedInterfaces"`
			SecuritySchemes map[string]any `json:"securitySchemes"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
			t.Fatal(err)
		}
		if len(card.SupportedInterfaces) != 1 || card.SupportedInterfaces[0].URL != base {
			t.Fatalf("card interfaces = %+v, want [%s]", card.SupportedInterfaces, base)
		}
		if _, ok := card.SecuritySchemes["pairing"]; !ok {
			t.Fatalf("card securitySchemes = %+v, want pairing scheme preserved", card.SecuritySchemes)
		}
	})
}

func TestAgentMeshSocketPeersAndHTTPClient(t *testing.T) {
	t.Parallel()
	sock := filepath.Join(t.TempDir(), "am.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /mesh/service/discover", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("type"); got != "a2a" {
			http.Error(w, "bad type "+got, http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]string{
			{"peer_id": "12D3KooWSelf", "srv_name": "coder", "srv_description": "self"},
			{"peer_id": "12D3KooWRemote", "srv_name": "reviewer", "srv_description": "code reviewer"},
		})
	})
	mux.HandleFunc("GET /mesh/12D3KooWRemote/a2a/reviewer/.well-known/agent-card.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"name":        "reviewer",
			"description": "Reviews pull requests.",
		})
	})
	srv := &http.Server{Handler: mux}
	defer func() { _ = srv.Close() }()
	go func() { _ = srv.Serve(ln) }()

	am := &AgentMesh{
		AgentName:  "coder",
		SocketPath: sock,
		peerID:     "12D3KooWSelf",
	}

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
}

func TestAgentMeshWriteNodeConfig(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "agentmesh-node.yaml")
	am := &AgentMesh{
		AgentName:       "coder",
		Description:     "Coding agent",
		Public:          true,
		IngressAuthMode: "edge",
		IngressAllow:    []string{"alice@example.com"},
	}
	if err := am.writeNodeConfig(cfgPath, "http://127.0.0.1:41234"); err != nil {
		t.Fatalf("writeNodeConfig() error = %v", err)
	}
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, sub := range []string{"version: v1alpha1", "name: coder", "type: a2a", "auth_mode: edge", "alice@example.com"} {
		if !strings.Contains(text, sub) {
			t.Fatalf("agentmesh-node.yaml missing %q:\n%s", sub, text)
		}
	}
}

func TestAgentMeshBootstrapCardDuringStart(t *testing.T) {
	t.Parallel()
	// Open an unserved listener to simulate the window inside AgentMesh.start()
	// before a2a server receives privateLn and calls http.Serve.
	unservedLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unservedLn.Close() }()

	muxLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = muxLn.Close() }()

	am := &AgentMesh{
		AgentName:   "coder",
		Description: "Coding agent",
	}
	am.bootstrapping.Store(true)
	srv := am.newMuxServer(muxLn.Addr().String(), unservedLn.Addr().String(), "")
	defer func() { _ = srv.Close() }()
	go func() { _ = srv.Serve(muxLn) }()

	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + muxLn.Addr().String() + "/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("bootstrap GET agent-card.json failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var card map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
		t.Fatalf("decoding bootstrap card: %v", err)
	}
	if card["name"] != "coder" {
		t.Fatalf("card[name] = %v, want coder", card["name"])
	}
}
