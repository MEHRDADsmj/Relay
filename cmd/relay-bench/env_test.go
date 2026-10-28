package main

import "testing"

func TestParseBuildInfoMetrics(t *testing.T) {
	body := `# HELP relay_build_info Build information (always 1).
# TYPE relay_build_info gauge
relay_build_info{commit="abc123",go_version="go1.26.10",storage="postgres",version="dev"} 1
`
	got := parseBuildInfoMetrics(body)
	if got.Version != "dev" || got.Commit != "abc123" || got.Storage != "postgres" || got.GoVersion != "go1.26.10" {
		t.Fatalf("unexpected build info: %+v", got)
	}
}
