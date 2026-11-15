package cursorapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

/**
 * @brief Read real account and repository schemas through the official Bearer API routes.
 * @param t Test harness using fake credentials and a private upstream.
 */
func TestAccountAndRepositories(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer fake" || r.Header.Get("x-cursor-client-type") != "sdk" {
			t.Error("incorrect account request method or identity headers")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/v1/me":
			_, _ = w.Write([]byte(`{"userId":42,"userEmail":"test@example.invalid","apiKeyName":"test","createdAt":"2026-01-01","futureField":true}`))
		case "/v1/repositories":
			_, _ = w.Write([]byte(`{"items":[{"url":"https://example.invalid/repository"}]}`))
		default:
			t.Error("incorrect account endpoint")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer upstream.Close()
	api := NewClient(upstream.Client())
	api.CloudBaseURL = upstream.URL
	user, err := api.Account(context.Background(), "fake")
	if err != nil || user.GetUserId() != 42 || user.GetUserEmail() != "test@example.invalid" {
		t.Fatalf("account mapping failed: %v", err)
	}
	repositories, err := api.Repositories(context.Background(), "fake")
	if err != nil || len(repositories) != 1 || repositories[0].GetUrl() != "https://example.invalid/repository" {
		t.Fatalf("repository mapping failed: %v", err)
	}
}

/**
 * @brief Do not hide authentication failures or malformed account responses behind empty success.
 * @param t Test harness with private upstreams.
 */
func TestAccountErrors(t *testing.T) {
	for _, status := range []int{401, 403, 429, 502, 200} {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{malformed`))
		}))
		api := NewClient(upstream.Client())
		api.CloudBaseURL = upstream.URL
		_, err := api.Account(context.Background(), "fake")
		if err == nil || ((status == 401 || status == 403) != errors.Is(err, ErrCredentialRejected)) {
			t.Errorf("status=%d incorrectly classified: %v", status, err)
		}
		upstream.Close()
	}
}
