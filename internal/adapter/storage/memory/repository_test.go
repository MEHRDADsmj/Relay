package memory_test

import (
	"testing"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/storagetest"
	"github.com/MEHRDADsmj/relay/internal/app"
)

func TestMemoryRepository_Contract(t *testing.T) {
	storagetest.RunContractTests(t, func(t *testing.T) storagetest.Backend {
		t.Helper()
		return memory.New()
	})
}

func TestMemoryRepository_Phase3Contract(t *testing.T) {
	storagetest.RunPhase3Contract(t, func(t *testing.T) storagetest.ScheduleAPI {
		t.Helper()
		return memory.New()
	})
}

func TestMemoryLeaderElector_Contract(t *testing.T) {
	shared := memory.NewLeaderElector()
	storagetest.RunLeaderElectorContract(t, func(t *testing.T) app.LeaderElector {
		t.Helper()
		return shared
	})
}
