package main

import (
	"context"
	"fmt"
	"log"
	"time"

	containernetwork "github.com/moby/moby/api/types/network"
)

// verifyFixture checks the running private fixture, including the PostgreSQL
// module's effective settings. Configuration requests alone are not evidence.
func (e *soakEnvironment) verifyFixture(ctx context.Context) error {
	var fsync, syncCommit, fullPages string
	if err := e.pool.QueryRow(ctx, "SELECT current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes')").Scan(&fsync, &syncCommit, &fullPages); err != nil {
		return err
	}
	if fsync != "on" || syncCommit != "on" || fullPages != "on" {
		return fmt.Errorf("non-durable soak PostgreSQL: %s/%s/%s", fsync, syncCommit, fullPages)
	}
	pg, err := e.postgres.Inspect(ctx)
	if err != nil {
		return err
	}
	if pg.HostConfig == nil || pg.HostConfig.ShmSize < 512*1024*1024 {
		return fmt.Errorf("soak PostgreSQL shm below512MiB")
	}
	bindings := pg.HostConfig.PortBindings[containernetwork.MustParsePort("5432/tcp")]
	if len(bindings) == 0 {
		return fmt.Errorf("PostgreSQL has no explicit loopback binding")
	}
	for _, b := range bindings {
		if b.HostIP.String() != "127.0.0.1" {
			return fmt.Errorf("PostgreSQL port is not loopback")
		}
	}
	e.fixture = map[string]any{"postgres_container": e.postgres.GetContainerID(), "kafka_container": e.kafka.GetContainerID(), "postgres_shm_size_bytes": pg.HostConfig.ShmSize, "durability": map[string]string{"fsync": fsync, "synchronous_commit": syncCommit, "full_page_writes": fullPages}}
	return nil
}

// kafkaRestart stops only this run's broker. Start retains its log and published
// endpoint; readiness is checked before a successful fault can be recorded.
func (e *soakEnvironment) kafkaRestart(ctx context.Context, duration time.Duration) error {
	timeout := 5 * time.Second
	if err := e.kafka.Stop(ctx, &timeout); err != nil {
		return fmt.Errorf("stop owned Kafka: %w", err)
	}
	state, err := e.kafka.State(ctx)
	if err != nil {
		return err
	}
	if state.Running {
		return fmt.Errorf("kafka still running after stop")
	}
	log.Printf("kafka stopped container=%s running=%t", e.kafka.GetContainerID(), state.Running)
	if !waitContext(ctx, duration) {
		return ctx.Err()
	}
	if err := e.kafka.Start(ctx); err != nil {
		return fmt.Errorf("restart owned Kafka: %w", err)
	}
	if err := e.pingKafka(ctx); err != nil {
		return err
	}
	state, err = e.kafka.State(ctx)
	if err != nil {
		return err
	}
	if !state.Running || state.Paused {
		return fmt.Errorf("kafka restart state not ready")
	}
	log.Printf("kafka restarted container=%s running=%t", e.kafka.GetContainerID(), state.Running)
	return nil
}
