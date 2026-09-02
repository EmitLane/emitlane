package main

import (
	"fmt"
)

func versionCmd(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: emitlane version")
	}
	fmt.Printf("EmitLane %s\ncommit: %s\nbuilt: %s\n", version, commit, date)
	return nil
}
