package main

import (
	"errors"
	"fmt"
)

// Exit codes per phase-3 §7.
const (
	exitOK       = 0
	exitUsage    = 1
	exitAPI      = 2
	exitNotFound = 3
)

// cliError carries a process exit code.
type cliError struct {
	code int
	err  error
}

func (e *cliError) Error() string {
	if e == nil || e.err == nil {
		return "relayctl: error"
	}
	return e.err.Error()
}

func (e *cliError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func usageErrorf(format string, args ...any) error {
	return &cliError{code: exitUsage, err: fmt.Errorf(format, args...)}
}

func apiErrorf(format string, args ...any) error {
	return &cliError{code: exitAPI, err: fmt.Errorf(format, args...)}
}

func notFoundErrorf(format string, args ...any) error {
	return &cliError{code: exitNotFound, err: fmt.Errorf(format, args...)}
}

func exitCode(err error) int {
	if err == nil {
		return exitOK
	}
	var ce *cliError
	if errors.As(err, &ce) {
		return ce.code
	}
	return exitAPI
}
