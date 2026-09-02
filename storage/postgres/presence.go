package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	adminapi "github.com/emitlane/emitlane/internal/admin"
	"github.com/emitlane/emitlane/relay"
)

func (s *Store) RegisterRelay(ctx context.Context, presence relay.RelayPresence) error {
	const query = `
INSERT INTO emitlane.relay_instances
    (instance_id, hostname, version, started_at, last_heartbeat_at, stopped_at, ordering_capable)
VALUES ($1, $2, $3, $4, NOW(), NULL, $5)
ON CONFLICT (instance_id) DO UPDATE SET
    hostname = EXCLUDED.hostname,
    version = EXCLUDED.version,
    started_at = EXCLUDED.started_at,
    last_heartbeat_at = NOW(),
    stopped_at = NULL,
    ordering_capable = EXCLUDED.ordering_capable`
	if _, err := s.pool.Exec(ctx, query, presence.InstanceID, presence.Hostname, presence.Version,
		presence.StartedAt, presence.OrderingCapable); err != nil {
		return fmt.Errorf("register relay presence: %w", err)
	}
	return nil
}

func (s *Store) HeartbeatRelay(ctx context.Context, instanceID string) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE emitlane.relay_instances
SET last_heartbeat_at = NOW(), stopped_at = NULL
WHERE instance_id = $1`, instanceID)
	if err != nil {
		return fmt.Errorf("heartbeat relay: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("heartbeat relay: instance %q is not registered", instanceID)
	}
	return nil
}

func (s *Store) MarkRelayStopped(ctx context.Context, instanceID string) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE emitlane.relay_instances
SET last_heartbeat_at = NOW(), stopped_at = NOW()
WHERE instance_id = $1`, instanceID)
	if err != nil {
		return fmt.Errorf("mark relay stopped: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark relay stopped: instance %q is not registered", instanceID)
	}
	return nil
}

func (s *Store) ListRelays(ctx context.Context, staleAfter time.Duration) ([]adminapi.RelayInstance, error) {
	const query = `
SELECT instance_id, hostname, version, started_at, last_heartbeat_at, stopped_at,
       CASE
           WHEN stopped_at IS NOT NULL THEN 'stopped'
           WHEN last_heartbeat_at < NOW() - ($1 * INTERVAL '1 millisecond') THEN 'stale'
           ELSE 'active'
       END AS state
FROM emitlane.relay_instances
ORDER BY instance_id`
	rows, err := s.pool.Query(ctx, query, intervalMS(staleAfter))
	if err != nil {
		return nil, fmt.Errorf("list relays: %w", err)
	}
	defer rows.Close()
	instances := make([]adminapi.RelayInstance, 0)
	for rows.Next() {
		var instance adminapi.RelayInstance
		if err := rows.Scan(&instance.InstanceID, &instance.Hostname, &instance.Version,
			&instance.StartedAt, &instance.LastHeartbeatAt, &instance.StoppedAt, &instance.State); err != nil {
			return nil, fmt.Errorf("list relays scan: %w", err)
		}
		instances = append(instances, instance)
	}
	return instances, rows.Err()
}

func (s *Store) GetRelay(ctx context.Context, id string, staleAfter time.Duration) (adminapi.RelayInstance, error) {
	const query = `
SELECT instance_id, hostname, version, started_at, last_heartbeat_at, stopped_at,
       CASE
           WHEN stopped_at IS NOT NULL THEN 'stopped'
           WHEN last_heartbeat_at < NOW() - ($2 * INTERVAL '1 millisecond') THEN 'stale'
           ELSE 'active'
       END AS state
FROM emitlane.relay_instances
WHERE instance_id = $1`
	var instance adminapi.RelayInstance
	err := s.pool.QueryRow(ctx, query, id, intervalMS(staleAfter)).Scan(
		&instance.InstanceID, &instance.Hostname, &instance.Version,
		&instance.StartedAt, &instance.LastHeartbeatAt, &instance.StoppedAt, &instance.State,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return adminapi.RelayInstance{}, fmt.Errorf("%w: relay %q", adminapi.ErrNotFound, id)
	}
	if err != nil {
		return adminapi.RelayInstance{}, fmt.Errorf("get relay: %w", err)
	}
	return instance, nil
}
