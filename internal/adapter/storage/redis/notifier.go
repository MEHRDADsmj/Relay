package redis

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Notifier fans out Redis Pub/Sub wakeups (PSUBSCRIBE relay:notify:*) to per-queue subscribers.
type Notifier struct {
	client *goredis.Client
	keys   Keys
	log    *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}

	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

// NewNotifier starts a dedicated PSUBSCRIBE loop. Call Close to stop it.
func NewNotifier(ctx context.Context, client *goredis.Client, prefix string) *Notifier {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n := &Notifier{
		client: client,
		keys:   NewKeys(prefix),
		log:    slog.Default(),
		cancel: cancel,
		done:   make(chan struct{}),
		subs:   make(map[string]map[chan struct{}]struct{}),
	}
	go n.loop(runCtx)
	return n
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

// Close stops the Pub/Sub loop and waits for it to exit.
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
			n.log.WarnContext(ctx, "redis notifier: subscribe interrupted", "err", err)
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
	pubsub := n.client.PSubscribe(ctx, n.keys.NotifyPattern())
	defer func() { _ = pubsub.Close() }()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return goredis.ErrClosed
			}
			queue := queueFromNotifyChannel(n.keys.Prefix(), msg.Channel)
			if queue != "" {
				n.wake(queue)
			}
		}
	}
}

func queueFromNotifyChannel(prefix, channel string) string {
	want := prefix + ":notify:"
	if !strings.HasPrefix(channel, want) {
		return ""
	}
	return strings.TrimPrefix(channel, want)
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
