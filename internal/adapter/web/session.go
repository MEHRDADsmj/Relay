package web

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	sessionCookieName = "relay_ui_session"
	sessionTTL        = 12 * time.Hour
	sessionPath       = "/ui"
)

// SessionPayload is stored inside an AES-GCM encrypted cookie.
type SessionPayload struct {
	KeyID     string    `json:"key_id"`
	ExpiresAt time.Time `json:"exp"`
	CSRF      string    `json:"csrf"`
}

var (
	errSessionMissing = errors.New("session missing")
	errSessionInvalid = errors.New("session invalid")
	errSessionExpired = errors.New("session expired")
)

type sessionStore struct {
	gcm    cipher.AEAD
	secure bool
	clock  func() time.Time
}

func newSessionStore(key []byte, secure bool, clock func() time.Time) (*sessionStore, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("session key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if clock == nil {
		clock = time.Now
	}
	return &sessionStore{gcm: gcm, secure: secure, clock: clock}, nil
}

func (s *sessionStore) issue(keyID string) (SessionPayload, error) {
	csrf, err := randomToken(32)
	if err != nil {
		return SessionPayload{}, err
	}
	now := s.clock().UTC()
	return SessionPayload{
		KeyID:     keyID,
		ExpiresAt: now.Add(sessionTTL),
		CSRF:      csrf,
	}, nil
}

func (s *sessionStore) encode(p SessionPayload) (string, error) {
	plain, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, s.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := s.gcm.Seal(nonce, nonce, plain, nil)
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (s *sessionStore) decode(token string) (SessionPayload, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return SessionPayload{}, errSessionInvalid
	}
	ns := s.gcm.NonceSize()
	if len(raw) < ns {
		return SessionPayload{}, errSessionInvalid
	}
	plain, err := s.gcm.Open(nil, raw[:ns], raw[ns:], nil)
	if err != nil {
		return SessionPayload{}, errSessionInvalid
	}
	var p SessionPayload
	if err := json.Unmarshal(plain, &p); err != nil {
		return SessionPayload{}, errSessionInvalid
	}
	if p.CSRF == "" {
		return SessionPayload{}, errSessionInvalid
	}
	if !p.ExpiresAt.After(s.clock().UTC()) {
		return SessionPayload{}, errSessionExpired
	}
	return p, nil
}

func (s *sessionStore) setCookie(w http.ResponseWriter, p SessionPayload) error {
	token, err := s.encode(p)
	if err != nil {
		return err
	}
	maxAge := int(p.ExpiresAt.Sub(s.clock().UTC()).Seconds())
	if maxAge < 0 {
		maxAge = 0
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     sessionPath,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   s.secure,
		Expires:  p.ExpiresAt,
		MaxAge:   maxAge,
	})
	return nil
}

func (s *sessionStore) clearCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     sessionPath,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   s.secure,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0).UTC(),
	})
}

func (s *sessionStore) fromRequest(r *http.Request) (SessionPayload, error) {
	c, err := r.Cookie(sessionCookieName)
	if err != nil {
		return SessionPayload{}, errSessionMissing
	}
	return s.decode(c.Value)
}

func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
