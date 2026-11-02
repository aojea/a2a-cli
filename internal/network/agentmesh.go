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
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/agentmesh/api"
	"github.com/google/agentmesh/sdk/meshnet"
)

// AgentMeshHost is the virtual hostname for private Agent Mesh URLs routed
// in-process over authenticated libp2p streams.
const AgentMeshHost = "agentmesh.local"

// agentMeshCapKey matches auth.TailscaleCap so edge-verified Agent Mesh
// callers receive member access unless overridden in network.identities.
const agentMeshCapKey = "a2aproject.org/cap/a2a"

// meshSession abstracts the in-process meshnet.Server and A2A Endpoints so
// connection splitting, WhoIs mapping, and Peers discovery can be unit-tested
// in isolation.
type meshSession interface {
	WhoIs(remoteAddr string) (*meshnet.CallerInfo, bool)
	Discover(ctx context.Context, serviceType api.ServiceType, serviceName string) ([]*api.DiscoveredProvider, error)
	HTTPClient() *http.Client
	AttenuateToken(rule *api.TaskAuthorizationRule, ttl time.Duration, seal bool) (string, time.Time, error)
	Close() error
}

// AgentMesh is a [Provider] which joins the agent server to an Agent Mesh
// network in-process using sdk/meshnet: private P2P streams, cryptographically
// verified Biscuit caller identity, DHT service discovery, and optional public
// HTTP ingress (/ingress/<peer_id>/a2a/<name>) without an external sidecar.
type AgentMesh struct {
	AgentName   string
	Description string
	StateDir    string
	HubURL      string
	JoinToken   string
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

	mu         sync.Mutex
	started    atomic.Bool
	srv        meshSession
	epClose    func() error
	private    *splitListener
	public     *splitListener
	peerID     string
	privateURL *url.URL
	publicURL  *url.URL
	edgeConns  sync.Map // remoteAddr ("meshnet:...") -> struct{}
}

var _ Provider = (*AgentMesh)(nil)

// Name implements [Provider].
func (a *AgentMesh) Name() string { return "agentmesh" }

// ListenPrivate implements [Provider].
func (a *AgentMesh) ListenPrivate(ctx context.Context) (net.Listener, *url.URL, error) {
	a.mu.Lock()
	if !a.started.Load() {
		a.Private = true
	}
	a.mu.Unlock()
	if err := a.start(ctx); err != nil {
		return nil, nil, err
	}
	if a.private == nil || a.privateURL == nil {
		return nil, nil, ErrUnsupported
	}
	return a.private, a.privateURL, nil
}

// ListenPublic implements [Provider].
func (a *AgentMesh) ListenPublic(ctx context.Context) (net.Listener, *url.URL, error) {
	a.mu.Lock()
	if !a.started.Load() {
		a.Public = true
	}
	a.mu.Unlock()
	if err := a.start(ctx); err != nil {
		return nil, nil, err
	}
	if a.public == nil || a.publicURL == nil {
		return nil, nil, errors.New("agentmesh public ingress is not enabled")
	}
	return a.public, a.publicURL, nil
}

func (a *AgentMesh) start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started.Load() {
		return nil
	}

	hub, joinToken, allowInsecure, err := a.resolveEnrollment()
	if err != nil {
		return err
	}
	authMode, err := parseIngressAuthMode(a.Public, a.IngressAuthMode)
	if err != nil {
		return err
	}

	logf := a.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	srv := &meshnet.Server{
		StateDir:        a.StateDir,
		ControlPlaneURL: hub,
		BootstrapToken:  joinToken,
		AllowInsecure:   allowInsecure,
		OnCredentialLapse: func(err error) {
			slog.Warn("agentmesh credential lapsed; retrying with backoff", "error", err)
			logf("! agentmesh: credential lapsed (%v)", err)
		},
	}
	st, err := srv.Up(ctx)
	if err != nil {
		return errors.Join(fmt.Errorf("joining agentmesh: %w", err), srv.Close())
	}

	ep, err := srv.ListenA2A(ctx, meshnet.A2AOptions{
		Name:         a.AgentName,
		Description:  a.Description,
		Public:       a.Public,
		IngressAuth:  authMode,
		IngressAllow: a.IngressAllow,
	})
	if err != nil {
		return errors.Join(fmt.Errorf("registering a2a service on agentmesh: %w", err), srv.Close())
	}
	if a.Public && ep.PublicURL == nil {
		return errors.Join(
			errors.New("agentmesh public ingress was not admitted by the hub policy or no router advertises an ingress_url"),
			ep.Close(),
			srv.Close(),
		)
	}

	a.srv = srv
	a.epClose = ep.Close
	a.peerID = st.PeerID
	a.attachEndpoints(ep.PrivateListener, ep.PrivateURL, ep.PublicListener, ep.PublicURL)
	slog.Info("joined agentmesh", "peer_id", st.PeerID, "routers", st.AttachedRouters)
	a.started.Store(true)
	return nil
}

func (a *AgentMesh) attachEndpoints(privLn net.Listener, privURL *url.URL, pubLn net.Listener, pubURL *url.URL) {
	if a.Private && privLn != nil {
		a.private = newSplitListener(privLn.Addr(), nil)
		a.privateURL = privURL
		go a.pumpPrivate(privLn, a.private)
	} else if privLn != nil {
		if err := privLn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Debug("closing unused agentmesh private listener", "error", err)
		}
	}

	if a.Public && pubLn != nil {
		a.public = newSplitListener(pubLn.Addr(), nil)
		a.publicURL = pubURL
		go a.pumpPublic(pubLn, a.private, a.public, a.srv)
	} else if pubLn != nil {
		if err := pubLn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Debug("closing unused agentmesh public listener", "error", err)
		}
	}
}

func (a *AgentMesh) pumpPrivate(src net.Listener, dst *splitListener) {
	for {
		c, err := src.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Warn("agentmesh private listener stopped", "error", err)
			}
			return
		}
		go dst.deliver(c)
	}
}

// pumpPublic routes external ingress connections. Unauthenticated requests
// (passthrough pairing and anonymous agent-card discovery) go to pub; requests
// whose Session Biscuit was verified at the mesh edge go to priv (when active)
// with an edge capability marker so auth.Chain admits them without pairing codes.
func (a *AgentMesh) pumpPublic(src net.Listener, priv, pub *splitListener, srv meshSession) {
	for {
		c, err := src.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Warn("agentmesh public listener stopped", "error", err)
			}
			return
		}
		addr := c.RemoteAddr().String()
		if priv != nil && srv != nil {
			if _, verified := srv.WhoIs(addr); verified {
				a.edgeConns.Store(addr, struct{}{})
				wrapped := &trackedConn{
					Conn:    c,
					onClose: func() { a.edgeConns.Delete(addr) },
				}
				go priv.deliver(wrapped)
				continue
			}
		}
		go pub.deliver(c)
	}
}

func (a *AgentMesh) resolveEnrollment() (hub, joinToken string, allowInsecure bool, err error) {
	hub = strings.TrimSpace(a.HubURL)
	if hub == "" {
		hub = strings.TrimSpace(os.Getenv("AGENTMESH_HUB"))
	}
	joinToken = strings.TrimSpace(a.JoinToken)
	if joinToken == "" {
		joinToken = strings.TrimSpace(os.Getenv("AGENTMESH_JOIN_TOKEN"))
	}
	hasIdentity := hasStoredMeshIdentity(a.StateDir)
	if !hasIdentity && joinToken == "" {
		if hub == "" {
			return "", "", false, fmt.Errorf("agentmesh is not enrolled yet: pass --agentmesh-hub <url> and --agentmesh-token env:AGENTMESH_JOIN_TOKEN")
		}
		return "", "", false, fmt.Errorf("agentmesh has no stored identity in %s: pass --agentmesh-token env:AGENTMESH_JOIN_TOKEN", a.StateDir)
	}
	if !hasIdentity && joinToken != "" && hub == "" {
		return "", "", false, errors.New("agentmesh needs --agentmesh-hub <url> (or $AGENTMESH_HUB) to enroll with a join token")
	}

	effectiveHub := hub
	if effectiveHub == "" && a.StateDir != "" {
		if _, cred, readErr := meshnet.ReadStateDir(a.StateDir); readErr == nil && cred != nil {
			effectiveHub = cred.GetControlPlaneUrl()
		}
	}
	return hub, joinToken, isInsecureHTTPURL(effectiveHub), nil
}

func parseIngressAuthMode(public bool, raw string) (api.IngressAuthMode, error) {
	if !public {
		return api.IngressAuthMode_INGRESS_AUTH_MODE_UNSPECIFIED, nil
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "passthrough":
		return api.IngressAuthMode_INGRESS_AUTH_MODE_PASSTHROUGH, nil
	case "edge":
		return api.IngressAuthMode_INGRESS_AUTH_MODE_EDGE, nil
	default:
		return api.IngressAuthMode_INGRESS_AUTH_MODE_UNSPECIFIED, fmt.Errorf("unsupported agentmesh ingress auth mode %q: want passthrough or edge", raw)
	}
}

func hasStoredMeshIdentity(stateDir string) bool {
	if strings.TrimSpace(stateDir) == "" {
		return false
	}
	if fileExists(filepath.Join(stateDir, meshnet.StateDirCredentialFile)) {
		return true
	}
	return fileExists(filepath.Join(stateDir, "agent.db"))
}

func isInsecureHTTPURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Scheme, "http")
}

// WhoIs implements [Provider].
func (a *AgentMesh) WhoIs(_ context.Context, remoteAddr string) (*Peer, error) {
	if !a.started.Load() {
		return nil, ErrNoIdentity
	}
	a.mu.Lock()
	srv := a.srv
	a.mu.Unlock()
	if srv == nil {
		return nil, ErrNoIdentity
	}
	info, ok := srv.WhoIs(remoteAddr)
	if !ok || info == nil {
		return nil, ErrNoIdentity
	}
	id := info.Principal
	if id == "" {
		id = info.PeerID
	}
	if id == "" {
		return nil, ErrNoIdentity
	}
	peer := &Peer{
		ID:      id,
		Machine: info.PeerID,
	}
	if _, edge := a.edgeConns.Load(remoteAddr); edge {
		peer.Caps = map[string][]json.RawMessage{
			agentMeshCapKey: {json.RawMessage(`{"role":"member"}`)},
		}
	}
	return peer, nil
}

// Peers implements [Provider].
func (a *AgentMesh) Peers(ctx context.Context) ([]PeerInfo, error) {
	if !a.started.Load() {
		return nil, ErrUnsupported
	}
	a.mu.Lock()
	srv := a.srv
	selfPeer := a.peerID
	a.mu.Unlock()
	if srv == nil {
		return nil, ErrUnsupported
	}

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	found, err := srv.Discover(ctx, api.ServiceType_SERVICE_TYPE_A2A, "")
	if err != nil {
		return nil, fmt.Errorf("discovering a2a services on agentmesh: %w", err)
	}

	candidates := make([]PeerInfo, 0, len(found))
	for _, p := range found {
		if p.GetPeerId() == "" || p.GetSrvName() == "" || p.GetPeerId() == selfPeer {
			continue
		}
		candidates = append(candidates, PeerInfo{
			Name:        p.GetSrvName(),
			URL:         fmt.Sprintf("http://%s/mesh/%s/a2a/%s", AgentMeshHost, p.GetPeerId(), p.GetSrvName()),
			Description: p.GetSrvDescription(),
			Online:      true,
		})
	}
	return ProbeAgents(ctx, a.HTTPClient(), candidates), nil
}

// HTTPClient implements [Provider]. Requests to http://agentmesh.local/...
// travel in-process over authenticated meshnet libp2p streams; all other URLs
// use the standard HTTP transport.
func (a *AgentMesh) HTTPClient() *http.Client {
	return &http.Client{
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL != nil && strings.EqualFold(r.URL.Host, AgentMeshHost) {
				if !a.started.Load() {
					return nil, errors.New("agentmesh: provider is not running")
				}
				a.mu.Lock()
				srv := a.srv
				a.mu.Unlock()
				if srv == nil {
					return nil, errors.New("agentmesh: provider is not running")
				}
				return srv.HTTPClient().Transport.RoundTrip(r)
			}
			return http.DefaultTransport.RoundTrip(r)
		}),
	}
}

// AttenuateToken appends a validated TaskAuthorizationRule (tar_block) scoped
// to resources/scope onto this member's Biscuit in memory and seals the token
// so downstream holders cannot append further blocks.
func (a *AgentMesh) AttenuateToken(resources []string, scope string, ttl time.Duration) (string, time.Time, error) {
	if !a.started.Load() {
		return "", time.Time{}, ErrUnsupported
	}
	a.mu.Lock()
	srv := a.srv
	a.mu.Unlock()
	if srv == nil {
		return "", time.Time{}, ErrUnsupported
	}
	tar, err := api.BuildTARFromOAuthParams("a2a-cli", "", resources, scope, nil)
	if err != nil {
		return "", time.Time{}, err
	}
	if tar == nil {
		return "", time.Time{}, errors.New("at least one --resource or --scope is required to attenuate a mesh token")
	}
	return srv.AttenuateToken(tar, ttl, true)
}

// Close implements [Provider].
func (a *AgentMesh) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var errs []error
	if a.private != nil {
		if err := a.private.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
		a.private = nil
	}
	if a.public != nil {
		if err := a.public.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
		a.public = nil
	}
	if a.epClose != nil {
		if err := a.epClose(); err != nil {
			errs = append(errs, err)
		}
		a.epClose = nil
	}
	if a.srv != nil {
		if err := a.srv.Close(); err != nil {
			errs = append(errs, err)
		}
		a.srv = nil
	}
	a.started.Store(false)
	return errors.Join(errs...)
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

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
