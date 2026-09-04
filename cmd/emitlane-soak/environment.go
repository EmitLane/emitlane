package main

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	containernetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	pgstore "github.com/emitlane/emitlane/storage/postgres"
)

type soakEnvironment struct {
	postgres        testcontainers.Container
	kafka           testcontainers.Container
	pool            *pgxpool.Pool
	store           *pgstore.Store
	databaseURL     string
	brokers         []string
	postgresVersion string
}

func startEnvironment(ctx context.Context, runID string) (*soakEnvironment, error) {
	labels := map[string]string{"com.emitlane.soak.run_id": runID, "com.emitlane.soak.owned": "true"}
	pgContainer, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("emitlane"), tcpostgres.WithUsername("emitlane"), tcpostgres.WithPassword("emitlane"),
		tcpostgres.BasicWaitStrategies(), testcontainers.WithLabels(labels),
	)
	if err != nil {
		return nil, fmt.Errorf("start PostgreSQL: %w", err)
	}
	fail := func(err error) (*soakEnvironment, error) {
		_ = testcontainers.TerminateContainer(pgContainer)
		return nil, err
	}
	port, err := reservePort()
	if err != nil {
		return fail(err)
	}
	portText := strconv.Itoa(port)
	kafkaContainer, err := testcontainers.Run(ctx, kafkaImage,
		testcontainers.WithExposedPorts("9092/tcp"), testcontainers.WithLabels(labels),
		testcontainers.WithEnv(map[string]string{
			"CLUSTER_ID": "MkU3OEVBNTcwNTJENDM2Qk", "KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
			"KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093", "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://127.0.0.1:" + portText,
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT", "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
			"KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT", "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
		}),
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			if hostConfig.PortBindings == nil {
				hostConfig.PortBindings = containernetwork.PortMap{}
			}
			hostConfig.PortBindings[containernetwork.MustParsePort("9092/tcp")] = []containernetwork.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: portText}}
		}),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("9092/tcp").WithStartupTimeout(2*time.Minute)),
	)
	if err != nil {
		return fail(fmt.Errorf("start Kafka: %w", err))
	}
	conn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(kafkaContainer)
		return fail(err)
	}
	pool, err := pgxpool.New(ctx, conn)
	if err != nil {
		_ = testcontainers.TerminateContainer(kafkaContainer)
		return fail(err)
	}
	if err := pgstore.MigrateUp(ctx, pool); err != nil {
		pool.Close()
		_ = testcontainers.TerminateContainer(kafkaContainer)
		return fail(err)
	}
	store, err := pgstore.NewStore(pool)
	if err != nil {
		pool.Close()
		_ = testcontainers.TerminateContainer(kafkaContainer)
		return fail(err)
	}
	var pgVersion string
	if err := pool.QueryRow(ctx, `SHOW server_version`).Scan(&pgVersion); err != nil {
		pool.Close()
		_ = testcontainers.TerminateContainer(kafkaContainer)
		return fail(err)
	}
	env := &soakEnvironment{postgres: pgContainer, kafka: kafkaContainer, pool: pool, store: store, databaseURL: conn, brokers: []string{net.JoinHostPort("127.0.0.1", portText)}, postgresVersion: pgVersion}
	if err := env.createTopics(ctx, "emitlane-soak-ordered-"+runID, "emitlane-soak-unordered-"+runID); err != nil {
		env.close(context.Background())
		return nil, err
	}
	return env, nil
}

func reservePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func (e *soakEnvironment) createTopics(ctx context.Context, topics ...string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	responses, err := admin.CreateTopics(ctx, 8, 1, nil, topics...)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for topic, response := range responses {
		if response.Err != nil {
			return fmt.Errorf("create topic %s: %w", topic, response.Err)
		}
	}
	return nil
}

func (e *soakEnvironment) close(ctx context.Context) {
	if e == nil {
		return
	}
	if e.pool != nil {
		e.pool.Close()
	}
	if e.kafka != nil {
		_ = testcontainers.TerminateContainer(e.kafka, testcontainers.StopContext(ctx))
	}
	if e.postgres != nil {
		_ = testcontainers.TerminateContainer(e.postgres, testcontainers.StopContext(ctx))
	}
}

func (e *soakEnvironment) kafkaOutage(ctx context.Context, duration time.Duration) error {
	if err := e.setKafkaPaused(ctx, true); err != nil {
		return err
	}
	timer := time.NewTimer(duration)
	select {
	case <-ctx.Done():
		timer.Stop()
		resumeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = e.setKafkaPaused(resumeCtx, false)
		return ctx.Err()
	case <-timer.C:
	}
	if err := e.setKafkaPaused(ctx, false); err != nil {
		return err
	}
	return e.pingKafka(ctx)
}

func (e *soakEnvironment) restoreKafka(ctx context.Context) error {
	state, err := e.kafka.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect Kafka container: %w", err)
	}
	if state.Paused {
		if err := e.setKafkaPaused(ctx, false); err != nil {
			return err
		}
	} else if !state.Running {
		if err := e.kafka.Start(ctx); err != nil {
			return fmt.Errorf("start Kafka container: %w", err)
		}
	}
	return e.pingKafka(ctx)
}

func (e *soakEnvironment) setKafkaPaused(ctx context.Context, paused bool) error {
	client, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return fmt.Errorf("create Docker client: %w", err)
	}
	defer client.Close()
	if paused {
		if _, err := client.ContainerPause(ctx, e.kafka.GetContainerID(), mobyclient.ContainerPauseOptions{}); err != nil {
			return fmt.Errorf("pause Kafka container: %w", err)
		}
		return nil
	}
	if _, err := client.ContainerUnpause(ctx, e.kafka.GetContainerID(), mobyclient.ContainerUnpauseOptions{}); err != nil {
		return fmt.Errorf("unpause Kafka container: %w", err)
	}
	return nil
}

func (e *soakEnvironment) pingKafka(ctx context.Context) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return client.Ping(pingCtx)
}

func gitIdentity() (string, string) {
	branch, _ := exec.Command("git", "branch", "--show-current").Output()
	commit, _ := exec.Command("git", "rev-parse", "HEAD").Output()
	return strings.TrimSpace(string(branch)), strings.TrimSpace(string(commit))
}

func dockerVersion() string {
	out, err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}
