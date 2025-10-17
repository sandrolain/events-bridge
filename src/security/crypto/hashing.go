package crypto

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// ComputeSHA256 computes the SHA256 hash of a file
func ComputeSHA256(filePath string) (string, error) {
	f, err := os.Open(filePath) // #nosec G304 - filePath is validated before calling this function
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("failed to compute hash: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// VerifySHA256 verifies the SHA256 hash of a file against an expected value
func VerifySHA256(filePath string, expectedHash string) error {
	// Validate hash format (64 hex characters)
	if len(expectedHash) != 64 {
		return fmt.Errorf("invalid hash format: expected 64 hex characters, got %d", len(expectedHash))
	}

	// Ensure hash contains only hex characters
	for _, c := range expectedHash {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return fmt.Errorf("invalid hash format: contains non-hex character: %c", c)
		}
	}

	// Compute actual hash
	actualHash, err := ComputeSHA256(filePath)
	if err != nil {
		return fmt.Errorf("failed to compute file hash: %w", err)
	}

	// Compare hashes (case-insensitive)
	if !equalHashes(actualHash, expectedHash) {
		return fmt.Errorf("hash mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	return nil
}

// equalHashes compares two hex hashes in a case-insensitive manner
func equalHashes(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca := a[i]
		cb := b[i]
		// Convert to lowercase if uppercase
		if ca >= 'A' && ca <= 'F' {
			ca = ca - 'A' + 'a'
		}
		if cb >= 'A' && cb <= 'F' {
			cb = cb - 'A' + 'a'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// ComputeBytes computes the SHA256 hash of a byte slice
func ComputeBytes(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
