package main

import (
	"encoding/json"
	"os"
)

func loadPayload(payload, payloadFile string) (any, error) {
	if payload != "" && payloadFile != "" {
		return nil, usageErrorf("only one of --payload and --payload-file allowed")
	}
	var raw []byte
	switch {
	case payloadFile != "":
		b, err := os.ReadFile(payloadFile) //nolint:gosec // operator-supplied --payload-file path
		if err != nil {
			return nil, usageErrorf("read --payload-file: %v", err)
		}
		raw = b
	case payload != "":
		raw = []byte(payload)
	default:
		return nil, nil
	}
	if !json.Valid(raw) {
		return nil, usageErrorf("payload is not valid JSON")
	}
	return json.RawMessage(append([]byte(nil), raw...)), nil
}
