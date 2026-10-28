package main

import "testing"

func TestWantsVersion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"serve"}, false},
		{[]string{"--version"}, true},
		{[]string{"-version"}, true},
		{[]string{"version"}, true},
		{[]string{"serve", "--version"}, true},
	}
	for _, tc := range cases {
		if got := wantsVersion(tc.args); got != tc.want {
			t.Fatalf("wantsVersion(%v)=%v want %v", tc.args, got, tc.want)
		}
	}
}
