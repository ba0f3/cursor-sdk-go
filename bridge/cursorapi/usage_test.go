package cursorapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

/**
 * @brief Preserve exact backend counters and absent optional costs while escaping native identities and filters.
 * @param t Private fake public API with synthetic usage only.
 */
func TestAgentUsageBackendMapping(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/v1/agents/native%2Fgroup/usage" || r.URL.Query().Get("runId") != "usage+id&filter" || r.Header.Get("Authorization") != "Bearer fake" {
			t.Error("native identity or usage filter was not escaped")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"totalUsage":{"inputTokens":"9007199254740993","outputTokens":4,"cacheReadTokens":100,"reasoningTokens":0},"runs":[{"id":"usage+id&filter","usage":{"outputTokens":4},"cost":{"rawCostCents":1.5,"chargedCents":0}}]}`))
	}))
	defer upstream.Close()
	api := NewClient(upstream.Client())
	api.CloudBaseURL = upstream.URL
	usage, err := api.AgentUsage(context.Background(), "fake", "native/group", "usage+id&filter")
	if err != nil {
		t.Fatal(err)
	}
	if usage.GetUsage().GetInputTokens() != 9007199254740993 || usage.GetUsage().ReasoningTokens == nil || usage.Cost != nil || len(usage.Runs) != 1 || usage.Runs[0].Cost == nil || usage.Runs[0].Cost.GetChargedCents() != 0 {
		t.Fatal("backend precision, optional reasoning or cost presence changed")
	}
}

/**
 * @brief Reject missing or malformed usage instead of reporting fabricated zero counters.
 * @param t Table of private upstream responses without credentials or user data.
 */
func TestAgentUsageFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
	}{
		{"authentication", 401, `{}`},
		{"capacity", 429, `{}`},
		{"malformed", 200, `{private-upstream-marker`},
		{"missing counters", 200, `{"runs":[]}`},
		{"null counters", 200, `{"totalUsage":null,"runs":[]}`},
		{"missing runs", 200, `{"totalUsage":{}}`},
		{"missing run identity", 200, `{"totalUsage":{},"runs":[{"usage":{}}]}`},
		{"invalid counters", 200, `{"totalUsage":{"inputTokens":"private-upstream-marker"},"runs":[]}`},
		{"invalid cost", 200, `{"totalUsage":{},"cost":{"chargedCents":"private-upstream-marker"},"runs":[]}`},
		{"oversized", 200, strings.Repeat(" ", (2<<20)+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer upstream.Close()
			api := NewClient(upstream.Client())
			api.CloudBaseURL = upstream.URL
			usage, err := api.AgentUsage(context.Background(), "fake", "native", "")
			if err == nil || usage != nil || strings.Contains(err.Error(), "private-upstream-marker") {
				t.Fatalf("incorrect usage failure or response content disclosure: %v", err)
			}
			if (test.status == 401) != errors.Is(err, ErrCredentialRejected) {
				t.Fatal("incorrect authentication classification")
			}
		})
	}
}
