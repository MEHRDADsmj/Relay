package sdk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  string
		forced  TransportKind
		want    TransportKind
		wantErr string
	}{
		{name: "http", target: "http://localhost:7420", want: TransportHTTP},
		{name: "https", target: "https://localhost:7420", want: TransportHTTP},
		{name: "grpc", target: "grpc://localhost:7421", want: TransportGRPC},
		{name: "grpcs", target: "grpcs://localhost:7421", want: TransportGRPC},
		{name: "force grpc on http url", target: "http://localhost:7420", forced: TransportGRPC, want: TransportGRPC},
		{name: "force http on grpc url", target: "grpc://localhost:7421", forced: TransportHTTP, want: TransportHTTP},
		{name: "ftp", target: "ftp://example.com", wantErr: "unsupported target scheme"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := detectKind(tt.target, tt.forced)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseGRPCTarget(t *testing.T) {
	t.Parallel()

	addr, tls, err := parseGRPCTarget("grpc://127.0.0.1:7421")
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:7421", addr)
	require.False(t, tls)

	addr, tls, err = parseGRPCTarget("grpcs://relay.example:443")
	require.NoError(t, err)
	require.Equal(t, "relay.example:443", addr)
	require.True(t, tls)

	addr, tls, err = parseGRPCTarget("passthrough:///bufnet")
	require.NoError(t, err)
	require.Equal(t, "passthrough:///bufnet", addr)
	require.False(t, tls)
}

func TestBuildTransport_UnsupportedScheme(t *testing.T) {
	t.Parallel()
	_, err := buildTransport("ftp://x", clientOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported target scheme")
}
