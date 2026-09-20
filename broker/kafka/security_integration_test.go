//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	containernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/emitlane/emitlane/broker"
)

// This broker has three loopback-only client listeners. The unexposed internal
// plaintext listener and permissive test superusers are fixture conveniences,
// never a deployment example. One container serves the matrix sequentially.
func startSecurityKafka(t *testing.T, files securityCertificates) map[string]string {
	t.Helper()
	ports := make(map[string]string)
	bindings := containernetwork.PortMap{}
	for name, internal := range map[string]string{"TLS": "9092", "MTLS": "9094", "SASL": "9095"} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		ports[name] = "127.0.0.1:" + port
		bindings[containernetwork.MustParsePort(internal+"/tcp")] = []containernetwork.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port}}
	}
	properties := fmt.Sprintf(`process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
controller.listener.names=CONTROLLER
listeners=TLS://:9092,CONTROLLER://:9093,MTLS://:9094,SASL://:9095,INTERNAL://:9096
advertised.listeners=TLS://%s,MTLS://%s,SASL://%s,INTERNAL://localhost:9096
listener.security.protocol.map=TLS:SSL,MTLS:SSL,SASL:SASL_SSL,CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
log.dirs=/tmp/security-kafka-data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
num.partitions=1
auto.create.topics.enable=false
ssl.keystore.type=PEM
ssl.keystore.location=/tmp/security-server.pem
ssl.truststore.type=PEM
ssl.truststore.location=/tmp/security-ca.pem
listener.name.mtls.ssl.client.auth=required
sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
listener.name.sasl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_testuser="test-password" user_denieduser="denied-password";
listener.name.sasl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.sasl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:ANONYMOUS;User:CN=test-client;User:testuser
allow.everyone.if.no.acl.found=false
`, ports["TLS"], ports["MTLS"], ports["SASL"])
	// All material is generated for this test. PKCS8 key + certificate is Kafka's PEM keystore format.
	key, err := os.ReadFile(files.key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := os.ReadFile(files.cert)
	if err != nil {
		t.Fatal(err)
	}
	startup := `/opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /tmp/security.properties --add-scram 'SCRAM-SHA-256=[name=testuser,password=test-password]' --add-scram 'SCRAM-SHA-512=[name=testuser,password=test-password]'
exec /opt/kafka/bin/kafka-server-start.sh /tmp/security.properties`
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := testcontainers.Run(ctx, "apache/kafka:4.3.1",
		testcontainers.WithEntrypoint("/bin/bash", "-ec"),
		testcontainers.WithCmd(startup),
		testcontainers.WithEnv(map[string]string{"KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m"}),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{Reader: strings.NewReader(properties), ContainerFilePath: "/tmp/security.properties", FileMode: 0o644},
			testcontainers.ContainerFile{Reader: strings.NewReader(string(key) + string(cert)), ContainerFilePath: "/tmp/security-server.pem", FileMode: 0o644},
			testcontainers.ContainerFile{HostFilePath: files.ca, ContainerFilePath: "/tmp/security-ca.pem", FileMode: 0o644},
		),
		testcontainers.WithExposedPorts("9092/tcp", "9094/tcp", "9095/tcp"),
		testcontainers.WithHostConfigModifier(func(cfg *container.HostConfig) {
			cfg.PortBindings = bindings
			cfg.Memory = 1536 * 1024 * 1024
			cfg.NanoCPUs = 2_000_000_000
		}),
		testcontainers.WithWaitStrategy(wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute)),
	)
	if c != nil {
		t.Cleanup(func() {
			if err := testcontainers.TerminateContainer(c); err != nil {
				t.Errorf("terminate security Kafka: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return ports
}

func securityTestPublisher(t *testing.T, address string, security SecurityConfig) *Publisher {
	t.Helper()
	pub, err := NewPublisher(Config{Brokers: []string{address}, PublishTimeout: 5 * time.Second, Security: security})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

func securityTestSource(t *testing.T, address, topic string, security SecurityConfig) *consumerSource {
	t.Helper()
	factory, err := NewConsumerFactory(ConsumerConfig{Brokers: []string{address}, Topics: []string{topic}, Group: topic,
		SessionTimeout: 6 * time.Second, RebalanceTimeout: 15 * time.Second, FetchMaxWait: 100 * time.Millisecond, Security: security})
	if err != nil {
		t.Fatal(err)
	}
	source, err := factory.NewSource("worker", securityListener{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(source.Close)
	return source.(*consumerSource)
}

func TestKafkaSecurityIntegration(t *testing.T) {
	files := makeSecurityCertificates(t, false)
	ports := startSecurityKafka(t, files)
	base := SecurityConfig{TLS: TLSConfig{Enabled: true, CAFile: files.ca}}
	adminPublisher := securityTestPublisher(t, ports["TLS"], base)
	admin := kadm.NewClient(adminPublisher.client)
	for _, mode := range []string{"TLS", "MTLS", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			security := base
			address := ports[mode]
			if mode == "MTLS" {
				security.TLS.CertFile, security.TLS.KeyFile = files.cert, files.key
			} else if mode != "TLS" {
				address = ports["SASL"]
				security.SASL = SASLConfig{Mechanism: SASLMechanism(mode), Username: "testuser", Password: "test-password"}
			}
			topic := "security-" + strings.ToLower(mode)
			if _, err := admin.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
				t.Fatal(err)
			}
			pub := securityTestPublisher(t, address, security)
			if err := pub.Ping(ctx); err != nil {
				t.Fatal(err)
			}
			if err := pub.Publish(ctx, broker.Message{ID: topic, Destination: topic, Payload: []byte("secure record")}); err != nil {
				t.Fatal(err)
			}
			source := securityTestSource(t, address, topic, security)
			record, err := source.Poll(ctx)
			if err != nil || string(record.Payload) != "secure record" {
				t.Fatalf("secure consumption failed: %v", err)
			}
			if err := source.Commit(ctx, record); err != nil {
				t.Fatal(err)
			}
			source.AllowRebalance()
			// A fresh member of the same group must resume after the committed record.
			source.Close()
			next := securityTestSource(t, address, topic, security)
			if err := pub.Publish(ctx, broker.Message{ID: topic + "-next", Destination: topic, Payload: []byte("next record")}); err != nil {
				t.Fatal(err)
			}
			nextRecord, err := next.Poll(ctx)
			if err != nil || nextRecord.Offset != record.Offset+1 || string(nextRecord.Payload) != "next record" {
				t.Fatalf("committed offset was not recovered: %v", err)
			}
			next.AllowRebalance()
		})
	}
	for _, mode := range []SASLMechanism{SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512} {
		t.Run("reject-password-"+string(mode), func(t *testing.T) {
			security := base
			security.SASL = SASLConfig{Mechanism: mode, Username: "testuser", Password: "wrong-secret"}
			pub := securityTestPublisher(t, ports["SASL"], security)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := pub.Ping(ctx); !errors.Is(err, kerr.SaslAuthenticationFailed) || strings.Contains(err.Error(), "wrong-secret") {
				t.Fatalf("expected sanitized SASL rejection: %v", err)
			}
			// Give consumer authentication its own budget, independent of Ping.
			consumerCtx, consumerCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer consumerCancel()
			source := securityTestSource(t, ports["SASL"], "security-tls", security)
			if _, err := source.Poll(consumerCtx); !errors.Is(err, kerr.SaslAuthenticationFailed) || strings.Contains(err.Error(), "wrong-secret") {
				t.Fatalf("expected consumer SASL rejection: %v", err)
			}
		})
	}
	t.Run("ACL-rejection", func(t *testing.T) {
		security := base
		security.SASL = SASLConfig{Mechanism: SASLPlain, Username: "denieduser", Password: "denied-password"}
		pub := securityTestPublisher(t, ports["SASL"], security)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		err := pub.Publish(ctx, broker.Message{ID: "denied", Destination: "security-tls", Payload: []byte("denied")})
		if !broker.IsPermanent(err) || !errors.Is(err, kerr.TopicAuthorizationFailed) {
			t.Fatalf("topic ACL rejection lost permanent classification: %v", err)
		}
		source := securityTestSource(t, ports["SASL"], "security-tls", security)
		if _, err := source.Poll(ctx); !errors.Is(err, kerr.TopicAuthorizationFailed) && !errors.Is(err, kerr.GroupAuthorizationFailed) {
			t.Fatalf("consumer ACL rejection: %v", err)
		}
	})
	for _, name := range []string{"wrong-host", "untrusted-CA", "missing-client-cert"} {
		t.Run(name, func(t *testing.T) {
			security, address := base, ports["TLS"]
			switch name {
			case "wrong-host":
				security.TLS.ServerName = "wrong.test"
			case "untrusted-CA":
				security.TLS.CAFile = makeSecurityCertificates(t, false).ca
			case "missing-client-cert":
				address = ports["MTLS"]
			}
			pub := securityTestPublisher(t, address, security)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := pub.Ping(ctx); err == nil {
				t.Fatal("invalid TLS identity was accepted")
			}
		})
	}
	t.Run("password-file-recreation", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "password")
		if err := os.WriteFile(file, []byte("wrong-secret\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		security := base
		security.SASL = SASLConfig{Mechanism: SASLSCRAMSHA512, Username: "testuser", PasswordFile: file}
		old := securityTestPublisher(t, ports["SASL"], security)
		if err := os.WriteFile(file, []byte("test-password\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := old.Ping(ctx); !errors.Is(err, kerr.SaslAuthenticationFailed) {
			t.Fatalf("existing client unexpectedly reread password: %v", err)
		}
		fresh := securityTestPublisher(t, ports["SASL"], security)
		if err := fresh.Ping(ctx); err != nil {
			t.Fatalf("recreated client did not load replacement password: %v", err)
		}
	})
}
