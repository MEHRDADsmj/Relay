package memory_test

import (
	"testing"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/storagetest"
)

func TestMemoryRepository_Contract(t *testing.T) {
	storagetest.RunContractTests(t, func(t *testing.T) storagetest.Backend {
		t.Helper()
		return memory.New()
	})
}
