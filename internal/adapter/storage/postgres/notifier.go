package postgres

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const notifyChannel = "relay_jobs"

// Notifier fans out Postgres LISTEN/NOTIFY wakeups to per-queue subscribers.
type Notifier struct {
	pool   *pgxpool.Pool
	log    *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}

	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

// NewNotifier starts a dedicated LISTEN loop on pool. Call Close to stop it.
// The listen loop is decoupled from ctx cancellation (survives request scopes);
// call Close to stop it.
func NewNotifier(ctx context.Context, pool *pgxpool.Pool) (*Notifier, error) {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n := &Notifier{
		pool:   pool,
		log:    slog.Default(),
		cancel: cancel,
		done:   make(chan struct{}),
		subs:   make(map[string]map[chan struct{}]struct{}),
	}
	go n.loop(runCtx)
	return n, nil
}

// Subscribe returns a buffered-1 wakeup channel for queue and an unsubscribe func.
func (n *Notifier) Subscribe(_ context.Context, queue string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	if n.subs[queue] == nil {
		n.subs[queue] = make(map[chan struct{}]struct{})
	}
	n.subs[queue][ch] = struct{}{}
	n.mu.Unlock()

	var once sync.Once
	unsub := func() {
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			if m := n.subs[queue]; m != nil {
				delete(m, ch)
				if len(m) == 0 {
					delete(n.subs, queue)
				}
			}
			close(ch)
		})
	}
	return ch, unsub
}

// Close stops the LISTEN loop and waits for it to exit.
func (n *Notifier) Close() {
	n.cancel()
	<-n.done
}

func (n *Notifier) loop(ctx context.Context) {
	defer close(n.done)
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		err := n.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			n.log.Warn("postgres notifier: listen interrupted", "err", err)
		}
		n.broadcastAll()
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (n *Notifier) listenOnce(ctx context.Context) error {
	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		n.wake(notification.Payload)
	}
}

func (n *Notifier) wake(queue string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs[queue] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (n *Notifier) broadcastAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, m := range n.subs {
		for ch := range m {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}
