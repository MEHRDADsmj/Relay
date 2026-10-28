package storagetest

import "github.com/MEHRDADsmj/relay/internal/app"

// Backend is a storage adapter that implements every Phase 2 port.
type Backend interface {
	app.JobRepository
	app.QueueRepository
	app.MaintenanceRepository
	app.JobNotifier
}
