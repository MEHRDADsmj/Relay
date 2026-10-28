package domain

import "testing"

func TestPrincipalCan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		scopes []Scope
		need   Scope
		want   bool
	}{
		{name: "exact producer", scopes: []Scope{ScopeProducer}, need: ScopeProducer, want: true},
		{name: "exact worker", scopes: []Scope{ScopeWorker}, need: ScopeWorker, want: true},
		{name: "exact admin", scopes: []Scope{ScopeAdmin}, need: ScopeAdmin, want: true},
		{name: "admin implies producer", scopes: []Scope{ScopeAdmin}, need: ScopeProducer, want: true},
		{name: "admin implies worker", scopes: []Scope{ScopeAdmin}, need: ScopeWorker, want: true},
		{name: "producer cannot worker", scopes: []Scope{ScopeProducer}, need: ScopeWorker, want: false},
		{name: "worker cannot producer", scopes: []Scope{ScopeWorker}, need: ScopeProducer, want: false},
		{name: "producer cannot admin", scopes: []Scope{ScopeProducer}, need: ScopeAdmin, want: false},
		{name: "empty scopes", scopes: nil, need: ScopeProducer, want: false},
		{name: "multi includes need", scopes: []Scope{ScopeProducer, ScopeWorker}, need: ScopeWorker, want: true},
		{name: "multi misses need", scopes: []Scope{ScopeProducer, ScopeWorker}, need: ScopeAdmin, want: false},
		{name: "admin among others", scopes: []Scope{ScopeProducer, ScopeAdmin}, need: ScopeWorker, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := Principal{KeyID: "k1", Name: "n", Scopes: tt.scopes}
			if got := p.Can(tt.need); got != tt.want {
				t.Fatalf("Can(%q) = %v, want %v", tt.need, got, tt.want)
			}
		})
	}
}

func TestScopeConstants(t *testing.T) {
	t.Parallel()

	if ScopeProducer != "producer" || ScopeWorker != "worker" || ScopeAdmin != "admin" {
		t.Fatalf("unexpected scope constants: %q %q %q", ScopeProducer, ScopeWorker, ScopeAdmin)
	}
}
