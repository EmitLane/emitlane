package main

import "testing"

func TestCommandExitCode(t *testing.T) {
	t.Parallel()
	var err error = commandExit(2)
	coded, ok := err.(interface{ ExitCode() int })
	if !ok || coded.ExitCode() != 2 || err.Error() != "" {
		t.Fatalf("command exit contract: err=%v coded=%t", err, ok)
	}
}
