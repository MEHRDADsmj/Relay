package app

import "context"

// NopNotifier is a JobNotifier that never delivers wakeups.
// Useful for servers/tests that do not use LeaseWait long-polling.
type NopNotifier struct{}

// Subscribe implements JobNotifier.
func (NopNotifier) Subscribe(context.Context, string) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, func() {}
}
