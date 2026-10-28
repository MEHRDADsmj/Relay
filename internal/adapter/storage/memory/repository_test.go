package memory_test

import (
	"testing"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/storagetest"
	"github.com/MEHRDADsmj/relay/internal/app"
)

func TestMemoryRepository_Contract(t *testing.T) {
	storagetest.RunContractTests(t, func(t *testing.T) app.JobRepository {
		t.Helper()
		return memory.New()
	})
}
