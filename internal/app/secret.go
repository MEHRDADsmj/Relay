package app

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"strings"
)

const (
	apiKeyPrefixLen = 8
	apiKeyRandBytes = 32
	base62Alphabet  = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// GenerateAPIKeySecret returns a secret of the form rly_<8-char prefix>_<base62(32 random bytes)>
// and the prefix segment used for lookup.
func GenerateAPIKeySecret() (secret, prefix string, err error) {
	prefixBytes := make([]byte, 6) // enough entropy for 8 base62 chars
	if _, err := rand.Read(prefixBytes); err != nil {
		return "", "", fmt.Errorf("generate prefix: %w", err)
	}
	prefix = encodeBase62(prefixBytes)
	if len(prefix) < apiKeyPrefixLen {
		prefix = (prefix + strings.Repeat("0", apiKeyPrefixLen))[:apiKeyPrefixLen]
	} else {
		prefix = prefix[:apiKeyPrefixLen]
	}

	raw := make([]byte, apiKeyRandBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", "", fmt.Errorf("generate secret body: %w", err)
	}
	body := encodeBase62(raw)
	secret = "rly_" + prefix + "_" + body
	return secret, prefix, nil
}

// ParseAPIKeySecret extracts the prefix from a full secret. Returns ok=false if malformed.
func ParseAPIKeySecret(secret string) (prefix string, ok bool) {
	if !strings.HasPrefix(secret, "rly_") {
		return "", false
	}
	rest := strings.TrimPrefix(secret, "rly_")
	i := strings.IndexByte(rest, '_')
	if i != apiKeyPrefixLen {
		return "", false
	}
	return rest[:apiKeyPrefixLen], true
}

// HashAPIKeySecret returns SHA-256(secret).
func HashAPIKeySecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

func encodeBase62(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	digits := make([]byte, 0, len(b)*2)
	n := append([]byte(nil), b...)
	for len(n) > 0 && !allZero(n) {
		var rem byte
		for i := 0; i < len(n); i++ {
			cur := uint16(n[i]) + uint16(rem)<<8
			n[i] = byte(cur / 62)
			rem = byte(cur % 62)
		}
		digits = append(digits, base62Alphabet[rem])
		start := 0
		for start < len(n) && n[start] == 0 {
			start++
		}
		n = n[start:]
	}
	if len(digits) == 0 {
		return string(base62Alphabet[0])
	}
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits)
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
