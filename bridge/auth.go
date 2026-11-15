package cursorbridgeruntime

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"os"
	"strings"
)

/**
 * @brief Loopback bearer gate for sdk.v1 Connect RPCs.
 */
type authGate struct {
	token string
}

/**
 * @brief Create a fresh per-process bridge bearer token file.
 * @param dir Directory that will host the token file.
 * @return Token bytes trimmed for Authorization headers.
 * @return Absolute path to the mode-0600 token file.
 * @return I/O or permission errors from token generation or file write.
 */
func writeAuthTokenFile(dir string) (token string, path string, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	token = strings.TrimSpace(base64.RawURLEncoding.EncodeToString(raw))
	path = dir + "/auth-token"
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		return "", "", err
	}
	return token, path, nil
}

func (g *authGate) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if g == nil || g.token == "" {
			http.Error(w, "bridge auth is unavailable", http.StatusInternalServerError)
			return
		}
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) || strings.TrimSpace(auth[len(prefix):]) != g.token {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"missing or invalid bridge token"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func requireBearer(h http.Handler, token string) http.Handler {
	if strings.TrimSpace(token) == "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "bridge token is not configured", http.StatusInternalServerError)
		})
	}
	return (&authGate{token: token}).middleware(h)
}
