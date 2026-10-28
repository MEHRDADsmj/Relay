package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

// Environment is the hardware/software block printed with every report.
type Environment struct {
	GoVersion       string `json:"go_version"`
	GOMAXPROCS      int    `json:"gomaxprocs"`
	CPUModel        string `json:"cpu_model"`
	StorageBackend  string `json:"storage_backend"`
	ServerVersion   string `json:"server_version"`
	ServerCommit    string `json:"server_commit"`
	ServerGoVersion string `json:"server_go_version"`
}

func collectEnvironment(cfg config) Environment {
	env := Environment{
		GoVersion:  runtime.Version(),
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		CPUModel:   cpuModel(),
	}
	if cfg.StorageHint != "" {
		env.StorageBackend = cfg.StorageHint
	}
	if cfg.Admin != "" {
		if bi, err := scrapeBuildInfo(cfg.Admin); err == nil {
			if bi.Storage != "" {
				env.StorageBackend = bi.Storage
			}
			env.ServerVersion = bi.Version
			env.ServerCommit = bi.Commit
			env.ServerGoVersion = bi.GoVersion
		}
	}
	if env.StorageBackend == "" {
		env.StorageBackend = "unknown"
	}
	if env.ServerVersion == "" {
		env.ServerVersion = "unknown"
	}
	return env
}

func cpuModel() string {
	if v := os.Getenv("PROCESSOR_IDENTIFIER"); v != "" {
		return v
	}
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					return strings.TrimSpace(parts[1])
				}
			}
		}
	}
	return fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
}

type buildInfo struct {
	Version   string
	Commit    string
	GoVersion string
	Storage   string
}

func scrapeBuildInfo(adminBase string) (buildInfo, error) {
	url := strings.TrimRight(adminBase, "/") + "/metrics"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return buildInfo{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return buildInfo{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return buildInfo{}, fmt.Errorf("metrics status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return buildInfo{}, err
	}
	return parseBuildInfoMetrics(string(body)), nil
}

func parseBuildInfoMetrics(body string) buildInfo {
	var out buildInfo
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "relay_build_info{") {
			continue
		}
		// relay_build_info{commit="...",go_version="...",storage="...",version="..."} 1
		start := strings.Index(line, "{")
		end := strings.Index(line, "}")
		if start < 0 || end <= start {
			continue
		}
		labels := line[start+1 : end]
		for _, part := range splitMetricLabels(labels) {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				continue
			}
			val := strings.Trim(kv[1], `"`)
			switch kv[0] {
			case "version":
				out.Version = val
			case "commit":
				out.Commit = val
			case "go_version":
				out.GoVersion = val
			case "storage":
				out.Storage = val
			}
		}
		break
	}
	return out
}

func splitMetricLabels(s string) []string {
	var parts []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			inQuote = !inQuote
			cur.WriteByte(c)
		case ',':
			if inQuote {
				cur.WriteByte(c)
			} else {
				parts = append(parts, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
