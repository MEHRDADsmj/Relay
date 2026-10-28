package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
)

var defaultStdinIsTTY = func() bool {
	return isatty.IsTerminal(os.Stdin.Fd()) || isatty.IsCygwinTerminal(os.Stdin.Fd())
}

// stdinIsTTY is overridable in tests.
var stdinIsTTY = defaultStdinIsTTY

// confirmDestructive prompts on a TTY unless yes is set.
// Non-TTY without --yes returns a usage error.
func confirmDestructive(action string, yes bool, in io.Reader, errOut io.Writer) error {
	if yes {
		return nil
	}
	if !stdinIsTTY() {
		return usageErrorf("%s: refusing non-interactive run without --yes", action)
	}
	_, _ = fmt.Fprintf(errOut, "%s? [y/N] ", action)
	sc := bufio.NewScanner(in)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return usageErrorf("confirm: %w", err)
		}
		return usageErrorf("confirm: aborted")
	}
	ans := strings.TrimSpace(strings.ToLower(sc.Text()))
	if ans == "y" || ans == "yes" {
		return nil
	}
	return usageErrorf("aborted")
}
