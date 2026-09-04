package main

import (
	"fmt"
	"io"
	"os"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stderr)
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "run":
		err = runCmd(args)
	case "migrate":
		err = migrateCmd(args)
	case "doctor":
		err = doctorCmd(args)
	case "version":
		err = versionCmd(args)
	case "dead":
		err = deadCmd(args)
	case "stats":
		err = statsCmd(args)
	case "events":
		err = eventsCmd(args)
	case "relay":
		err = relayCmd(args)
	case "replay":
		err = replayCmd(args)
	case "audit":
		err = auditCmd(args)
	case "ordering":
		err = orderingCmd(args)
	case "help", "-h", "--help":
		printUsage(os.Stdout)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		printUsage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "emitlane %s: %v\n", cmd, err)
		os.Exit(1)
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, `EmitLane — reliable event delivery for distributed systems.

Usage:
  emitlane run
  emitlane migrate up
  emitlane migrate down
  emitlane doctor
  emitlane dead list
  emitlane dead retry <event-id>
  emitlane stats [--json]
  emitlane events list [filters]
  emitlane events inspect <event-id> [--json] [--payload]
  emitlane relay status|pause|resume
  emitlane replay event <event-id> --reason reason
  emitlane replay range [filters] --reason reason [--execute]
  emitlane audit list [--json]
  emitlane ordering streams [--blocked] [--json]
  emitlane ordering inspect --destination name --key key [--json]
  emitlane ordering partitions [--json]
  emitlane version

Configuration is read from EMITLANE_* environment variables.
`)
}
