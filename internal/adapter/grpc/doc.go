// Package grpc is the inbound gRPC adapter for Relay app services.
//
// Wire with NewServer and Serve on RELAY_GRPC_ADDR (default :7421) from the
// server process (P3-T9). Adapter tests use bufconn with the memory store.
package grpc
