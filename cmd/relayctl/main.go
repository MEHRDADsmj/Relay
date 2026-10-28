// Command relayctl is the operator CLI for Relay (jobs, queues, schedules, API keys).
package main

import (
	"fmt"
	"os"
)

func main() {
	code := Execute(os.Args[1:])
	if code != exitOK {
		os.Exit(code)
	}
}

// Execute runs relayctl with args (no program name) and returns an exit code.
func Execute(args []string) int {
	root := newRoot()
	root.SetArgs(args)
	err := root.Execute()
	if err != nil {
		_, _ = fmt.Fprintln(root.ErrOrStderr(), err.Error())
		return exitCode(err)
	}
	return exitOK
}
