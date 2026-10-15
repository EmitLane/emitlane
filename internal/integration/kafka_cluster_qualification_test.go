//go:build integration

package integration_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/api/types/container"
	containernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/emitlane/emitlane/broker"
	managed "github.com/emitlane/emitlane/consumer"
	"github.com/emitlane/emitlane/integrity"
	"github.com/emitlane/emitlane/outbox"
	"github.com/emitlane/emitlane/relay"
)

// Explicitly separate from the single-broker endurance profile. Observe actual
// leader/ISR changes, then reconcile every committed identity and protected effect.
func TestKafkaClusterLeaderISRAndNetworkRecovery(t *testing.T) {
	if os.Getenv("EMITLANE_KAFKA_CLUSTER_QUALIFICATION") != "1" {
		t.Skip("opt-in three-broker leader, ISR and private-network qualification")
	}
	t.Setenv("EMITLANE_QUALIFICATION_DURATION", "3m") // durable PostgreSQL fixture
	e := *startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	clusterNetwork, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clusterNetwork.Remove(context.Background())
	clusterID := uuid.New()
	ports := make([]int, 3)
	aliases := make([]string, 3)
	for n := range ports {
		ports[n], err = reserveLocalPort()
		if err != nil {
			t.Fatal(err)
		}
		aliases[n] = fmt.Sprintf("broker-%d-%s", n+1, uuid.NewString()[:8])
	}
	voters := fmt.Sprintf("1@%s:9093,2@%s:9093,3@%s:9093", aliases[0], aliases[1], aliases[2])
	brokers := make([]testcontainers.Container, 3)
	e.brokers = nil
	for n := range brokers {
		n := n
		port := strconv.Itoa(ports[n])
		brokers[n], err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
			Image: "apache/kafka:4.3.1", ExposedPorts: []string{"9094/tcp"},
			Networks: []string{clusterNetwork.Name}, NetworkAliases: map[string][]string{clusterNetwork.Name: {aliases[n]}},
			Env: map[string]string{
				"CLUSTER_ID": base64.RawURLEncoding.EncodeToString(clusterID[:]), "KAFKA_NODE_ID": strconv.Itoa(n + 1),
				"KAFKA_PROCESS_ROLES": "broker,controller", "KAFKA_CONTROLLER_QUORUM_VOTERS": voters,
				"KAFKA_LISTENERS":                      "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:9094",
				"KAFKA_ADVERTISED_LISTENERS":           "INTERNAL://" + aliases[n] + ":9092,EXTERNAL://127.0.0.1:" + port,
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
				"KAFKA_INTER_BROKER_LISTENER_NAME":     "INTERNAL", "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "3", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "3",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "2", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
				"KAFKA_HEAP_OPTS": "-Xms128m -Xmx256m", "KAFKA_REPLICA_LAG_TIME_MAX_MS": "5000",
			},
			HostConfigModifier: func(c *container.HostConfig) {
				c.Memory = 768 * 1024 * 1024
				if c.PortBindings == nil {
					c.PortBindings = containernetwork.PortMap{}
				}
				c.PortBindings[containernetwork.MustParsePort("9094/tcp")] = []containernetwork.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port}}
			},
			WaitingFor: wait.ForListeningPort("9094/tcp").WithStartupTimeout(3 * time.Minute),
		}})
		if err != nil {
			t.Fatal(err)
		}
		defer testcontainers.TerminateContainer(brokers[n])
		e.brokers = append(e.brokers, net.JoinHostPort("127.0.0.1", port))
	}
	var starts errgroup.Group
	for _, c := range brokers {
		c := c
		starts.Go(func() error { return c.Start(ctx) })
	}
	if err := starts.Wait(); err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	topic, group, consumerName := "cluster-"+uuid.NewString(), "cluster-group-"+uuid.NewString(), "cluster-consumer-"+uuid.NewString()
	minISR := "2"
	created, err := admin.CreateTopics(ctx, 3, 3, map[string]*string{"min.insync.replicas": &minISR}, topic)
	if err != nil || created[topic].Err != nil {
		t.Fatalf("create replicated topic: %v / %v", err, created[topic].Err)
	}
	partition := func(requireISR int, priorLeader int32) kadm.PartitionDetail {
		t.Helper()
		deadline := time.Now().Add(75 * time.Second)
		for time.Now().Before(deadline) {
			details, err := admin.ListTopics(ctx, topic)
			if err == nil {
				p := details[topic].Partitions[0]
				all := len(details[topic].Partitions) == 3
				for _, other := range details[topic].Partitions {
					all = all && other.Err == nil && other.Leader > 0 && len(other.Replicas) == 3 && len(other.ISR) == requireISR
				}
				if all && p.Err == nil && p.Leader > 0 && len(p.Replicas) == 3 && len(p.ISR) == requireISR && p.Leader != priorLeader {
					t.Logf("cluster_partition leader=%d epoch=%d replicas=%v ISR=%v", p.Leader, p.LeaderEpoch, p.Replicas, p.ISR)
					return p
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("partition did not reach ISR=%d with leader !=%d", requireISR, priorLeader)
		return kadm.PartitionDetail{}
	}
	initial := partition(3, -1)
	if _, err := e.pool.Exec(ctx, `CREATE TABLE public.cluster_effect_progress(singleton BOOLEAN PRIMARY KEY, sequence BIGINT NOT NULL); INSERT INTO public.cluster_effect_progress VALUES(TRUE,0)`); err != nil {
		t.Fatal(err)
	}
	observer := &clusterPublishObserver{inner: e.publisher(t)}
	relayRun := launchQualificationRelay(e.newRelay(t, qualificationRelayConfig("cluster-relay"), observer, relay.FailureHooks{}))
	defer relayRun.stop(t)
	config := managedTestConfig(consumerName, "cluster-managed")
	config.Concurrency = 3
	handler := func(ctx context.Context, tx pgx.Tx, m managed.Message) error {
		if string(m.Payload) != m.EventID.String() {
			return fmt.Errorf("cluster payload/identity mismatch")
		}
		var stream string
		var sequence int64
		for _, h := range m.Headers {
			if h.Key == broker.HeaderOrderingKey {
				stream = string(h.Value)
			}
			if h.Key == broker.HeaderSequence {
				var err error
				sequence, err = strconv.ParseInt(string(h.Value), 10, 64)
				if err != nil {
					return err
				}
			}
		}
		if stream != "" {
			tag, err := tx.Exec(ctx, `UPDATE public.cluster_effect_progress SET sequence=$1 WHERE singleton=TRUE AND sequence=$1-1`, sequence)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("cluster business order violated at%d", sequence)
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO public.business_payments(order_id,amount) VALUES($1,1)`, m.EventID.String())
		return err
	}
	consumerRun := launchManaged(t, &e, config, newManagedInboxStore(t, &e), newManagedFactory(t, &e, topic, group), handler)
	defer consumerRun.stop(t)
	type expectedEvent struct {
		key      string
		sequence int64
	}
	expected := make(map[string]expectedEvent)
	enqueue := func(n int) {
		t.Helper()
		for range n {
			id := uuid.NewString()
			tx, err := e.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO public.business_orders(id,amount) VALUES($1,1)`, "cluster-"+id); err != nil {
				_ = tx.Rollback(ctx)
				t.Fatal(err)
			}
			want := expectedEvent{key: id}
			event := outbox.Event{ID: id, Destination: topic, Type: "cluster.input", Key: []byte(id), Payload: []byte(id)}
			if len(expected)%2 == 0 {
				want.key, want.sequence = "cluster-stream", int64(len(expected)/2+1)
				event.Key, event.OrderingKey, event.Sequence = []byte(want.key), want.key, want.sequence
			}
			_, err = e.writer.Enqueue(ctx, tx, event)
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			expected[id] = want
		}
	}
	drain := func() {
		t.Helper()
		deadline := time.Now().Add(75 * time.Second)
		for time.Now().Before(deadline) {
			var completed int
			err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.inbox_events i JOIN public.business_payments p ON p.order_id=i.event_id::text JOIN emitlane.outbox_events o ON o.id=i.event_id WHERE i.consumer=$1 AND i.status='processed' AND o.status='delivered'`, consumerName).Scan(&completed)
			if err == nil && completed == len(expected) {
				lag, err := qualificationKafkaSnapshot(ctx, e.brokers, topic, group)
				if err == nil && lag["lag"].(int64) == 0 {
					return
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("cluster did not finish %d committed events", len(expected))
	}
	enqueue(100)
	drain()
	leader := int(initial.Leader) - 1
	if err := brokers[leader].Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}
	partition(2, initial.Leader) // require actual failover before testing traffic
	enqueue(100)
	drain()
	if err := brokers[leader].Start(ctx); err != nil {
		t.Fatal(err)
	}
	partition(3, -1)
	minISR = "3"
	changed, err := admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "min.insync.replicas", Value: &minISR}}, topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range changed {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	current := partition(3, -1)
	follower := (int(current.Leader)) % 3
	if err := brokers[follower].Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}
	partition(2, -1)
	beforeErrors := observer.insufficientISR.Load()
	enqueue(20)
	deadline := time.Now().Add(20 * time.Second)
	for observer.insufficientISR.Load() == beforeErrors && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if observer.insufficientISR.Load() == beforeErrors {
		t.Fatal("did not observe a real insufficient-ISR publish rejection")
	}
	var unfinished int
	if err := e.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emitlane.outbox_events WHERE destination=$1 AND status<>'delivered'`, topic).Scan(&unfinished); err != nil || unfinished != 20 {
		t.Fatalf("unacknowledged Outbox events=%d, want20: %v", unfinished, err)
	}
	if err := brokers[follower].Start(ctx); err != nil {
		t.Fatal(err)
	}
	partition(3, -1)
	drain()
	minISR = "2"
	changed, err = admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "min.insync.replicas", Value: &minISR}}, topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range changed {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	current = partition(3, -1)
	follower = int(current.Leader) % 3
	containerID := brokers[follower].GetContainerID()
	// Only the freshly created test network and exact owned container are touched.
	if data, err := exec.CommandContext(ctx, "docker", "network", "disconnect", clusterNetwork.Name, containerID).CombinedOutput(); err != nil {
		t.Fatalf("disconnect test follower: %v: %s", err, data)
	}
	disconnected := true
	defer func() {
		if disconnected {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_ = exec.CommandContext(cleanupCtx, "docker", "network", "connect", "--alias", aliases[follower], clusterNetwork.Name, containerID).Run()
		}
	}()
	partition(2, -1)
	enqueue(100)
	drain()
	if data, err := exec.CommandContext(ctx, "docker", "network", "connect", "--alias", aliases[follower], clusterNetwork.Name, containerID).CombinedOutput(); err != nil {
		t.Fatalf("reconnect test follower: %v: %s", err, data)
	}
	disconnected = false
	partition(3, -1)
	consumerRun.stop(t)
	relayRun.stop(t)
	// A fresh independent reader audits every captured end offset, including duplicates.
	snapshot, err := qualificationKafkaSnapshot(ctx, e.brokers, topic, group)
	if err != nil || snapshot["lag"].(int64) != 0 {
		t.Fatalf("final group lag: %v / %v", snapshot, err)
	}
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	startsAt := make(map[int32]kgo.Offset)
	for p := range ends[topic] {
		startsAt[p] = kgo.NewOffset().AtStart()
	}
	audit, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: startsAt}))
	if err != nil {
		t.Fatal(err)
	}
	defer audit.Close()
	seen, positions := make(map[string]int), make(map[int32]int64)
	var orderedLast int64
	orderedPartition := int32(-1)
	for {
		complete := true
		for p, end := range ends[topic] {
			if end.Err != nil {
				t.Fatal(end.Err)
			}
			complete = complete && positions[p] >= end.Offset
		}
		if complete {
			break
		}
		fetched := audit.PollRecords(ctx, 100)
		if err := fetched.Err(); err != nil {
			t.Fatal(err)
		}
		for _, record := range fetched.Records() {
			id := headerValueNoTest(record, broker.HeaderEventID)
			want, ok := expected[id]
			if !ok || string(record.Key) != want.key || string(record.Value) != id {
				t.Fatalf("unexpected identity/key/payload in replicated Kafka audit: %s", id)
			}
			if want.sequence > 0 {
				got, err := strconv.ParseInt(headerValueNoTest(record, broker.HeaderSequence), 10, 64)
				if err != nil || got != want.sequence || headerValueNoTest(record, broker.HeaderOrderingKey) != want.key || got < orderedLast || got > orderedLast+1 {
					t.Fatalf("cluster ordered audit regression/gap/metadata mismatch at%s", id)
				}
				if orderedPartition != -1 && orderedPartition != record.Partition {
					t.Fatal("ordered stream changed Kafka partition")
				}
				orderedPartition, orderedLast = record.Partition, got
			}
			seen[id]++
			positions[record.Partition] = record.Offset + 1
		}
	}
	if len(seen) != len(expected) {
		t.Fatalf("audited unique records=%d expected=%d", len(seen), len(expected))
	}
	for id := range expected {
		var effect, business int
		var inboxState, outboxState string
		if err := e.pool.QueryRow(ctx, `SELECT p.amount,b.amount,i.status,o.status
FROM public.business_payments p
JOIN public.business_orders b ON b.id='cluster-'||p.order_id
JOIN emitlane.inbox_events i ON i.event_id=p.order_id::uuid AND i.consumer=$2
JOIN emitlane.outbox_events o ON o.id=i.event_id AND o.destination=$3
WHERE p.order_id=$1`, id, consumerName, topic).Scan(&effect, &business, &inboxState, &outboxState); err != nil || effect != 1 || business != 1 || inboxState != "processed" || outboxState != "delivered" {
			t.Fatalf("missing/wrong identity/effect/state for%s: %v", id, err)
		}
	}
	var payments, orders, inboxRows, outboxRows int
	if err := e.pool.QueryRow(ctx, `SELECT
(SELECT COUNT(*) FROM public.business_payments),
(SELECT COUNT(*) FROM public.business_orders WHERE id LIKE 'cluster-%'),
(SELECT COUNT(*) FROM emitlane.inbox_events WHERE consumer=$1),
(SELECT COUNT(*) FROM emitlane.outbox_events WHERE destination=$2)`, consumerName, topic).Scan(&payments, &orders, &inboxRows, &outboxRows); err != nil || payments != len(expected) || orders != len(expected) || inboxRows != len(expected) || outboxRows != len(expected) {
		t.Fatalf("extra/missing identities: payments=%d/orders=%d/inbox=%d/outbox=%d: %v", payments, orders, inboxRows, outboxRows, err)
	}
	var lastEffect int64
	if err := e.pool.QueryRow(ctx, `SELECT sequence FROM public.cluster_effect_progress WHERE singleton=TRUE`).Scan(&lastEffect); err != nil || lastEffect != int64(len(expected)/2) || orderedLast != lastEffect {
		t.Fatalf("cluster final ordered progress=%d/%d: %v", lastEffect, orderedLast, err)
	}
	verifier, err := integrity.NewVerifier(e.pool, integrity.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifier.Check(ctx, integrity.ModeFull)
	if err != nil || report.FindingsTruncated || report.Summary.Violations != 0 || report.Summary.InboxStaleLeases != 0 {
		t.Fatalf("cluster full integrity: %+v: %v", report, err)
	}
	t.Logf("cluster integrity=%+v", report)
	t.Logf("cluster PASS committed=%d protected_effects=%d insufficient_ISR_errors=%d leader_failover=true follower_network_disconnect=true final_ISR=3 final_lag=0", len(expected), len(expected), observer.insufficientISR.Load())
}

type clusterPublishObserver struct {
	inner           broker.Publisher
	insufficientISR atomic.Int64
}

func (p *clusterPublishObserver) Publish(ctx context.Context, m broker.Message) error {
	err := p.inner.Publish(ctx, m)
	if errors.Is(err, kerr.NotEnoughReplicas) || errors.Is(err, kerr.NotEnoughReplicasAfterAppend) {
		p.insufficientISR.Add(1)
	}
	return err
}

func (p *clusterPublishObserver) Close() error { return p.inner.Close() }
