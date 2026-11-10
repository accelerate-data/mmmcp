package mmmcp_test

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/obot-platform/mmmcp"
	"github.com/obot-platform/mmmcp/catalog"
	"github.com/obot-platform/mmmcp/config"
)

// Request-scoped discovery and partial authorization were built separately
// (upstream v0.2.1 and fork #1). This covers them together: a per-request
// catalog keeps the ready component listed while another waits for OAuth, and
// completing that OAuth surfaces the new tools without reusing state across a
// credential change.
func TestRequestScopedDiscoveryWithPartialAuthorization(t *testing.T) {
	for _, protocol := range []string{"2025-11-25", "2026-07-28"} {
		t.Run(protocol, func(t *testing.T) {
			ready := namedToolFixture(t, "ready")
			var authorized atomic.Bool
			first := namedToolFixture(t, "first")
			target, err := url.Parse(first.URL)
			if err != nil {
				t.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(target)
			gated := testHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !authorized.Load() || r.Header.Get("Authorization") == "Bearer different-account" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				proxy.ServeHTTP(w, r)
			}))

			var current atomic.Pointer[config.Config]
			current.Store(&config.Config{
				RequestScopedDiscovery: true,
				DeferAuthorization:     true,
				PartialAuthorization:   true,
				Servers: []config.Server{
					{Name: "first", URL: gated.URL},
					{Name: "ready", URL: ready.URL},
				},
			})
			composite, err := mmmcp.New(t.Context(), &config.Config{}, mmmcp.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer composite.Close()
			frontend := testHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := mmmcp.ContextWithConfigID(r.Context(), "scoped")
				ctx = mmmcp.ContextWithConfig(ctx, current.Load())
				composite.HTTPHandler().ServeHTTP(w, r.WithContext(ctx))
			}))
			changed := make(chan struct{}, 4)
			client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, &mcp.ClientOptions{
				ToolListChangedHandler: func(context.Context, *mcp.ToolListChangedRequest) {
					select {
					case changed <- struct{}{}:
					default:
					}
				},
			})
			session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: frontend.URL, HTTPClient: frontend.Client()}, &mcp.ClientSessionOptions{ProtocolVersion: protocol})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()

			names := listToolNames(t, session)
			if !slices.Contains(names, "ready__tool") || !slices.Contains(names, catalog.AuthorizationToolName) || hasToolFrom(names, "first") {
				t.Fatalf("initial tools = %v, want ready tool and authorization status only", names)
			}

			authorized.Store(true)
			status, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: catalog.AuthorizationToolName})
			if err != nil || status.IsError {
				t.Fatalf("status = %+v, error = %v", status, err)
			}
			if text := status.Content[0].(*mcp.TextContent).Text; strings.Contains(text, "first") {
				t.Fatalf("status after sign-in still lists first as pending: %q", text)
			}
			if protocol == "2025-11-25" {
				select {
				case <-changed:
				case <-time.After(5 * time.Second):
					t.Fatal("authorization completion did not notify the tool list under request-scoped discovery")
				}
			}
			names = listToolNames(t, session)
			if !slices.Contains(names, "first__tool") || !slices.Contains(names, "ready__tool") {
				t.Fatalf("tools after sign-in = %v, want first and ready tools", names)
			}
			assertToolText(t, session, "first__tool", "first")

			// A different account must authorize on its own; the earlier grant is not reused.
			next := *current.Load()
			next.Servers = slices.Clone(next.Servers)
			next.Servers[0].Headers = map[string]string{"Authorization": "Bearer different-account"}
			current.Store(&next)
			names = listToolNames(t, session)
			if hasToolFrom(names, "first") || !slices.Contains(names, catalog.AuthorizationToolName) || !slices.Contains(names, "ready__tool") {
				t.Fatalf("tools after credential change = %v, want first pending again and ready still listed", names)
			}
		})
	}
}

func listToolNames(t *testing.T, session *mcp.ClientSession) []string {
	t.Helper()
	result, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return toolNameList(result.Tools)
}

func hasToolFrom(names []string, component string) bool {
	return slices.ContainsFunc(names, func(name string) bool { return strings.HasPrefix(name, component+"__") })
}
