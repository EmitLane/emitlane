# Kafka connection security

This document defines the v0.7 development contract. Release qualification is
required before treating the development implementation as production-ready.

## Supported modes

Publisher and managed consumer share `kafka.SecurityConfig`. Its zero value
preserves the existing plaintext, unauthenticated connection for compatibility
with local development. Enabling TLS uses TLS 1.2 or later, certificate-chain
verification and broker-hostname verification. There is no verification bypass.

- TLS with the operating system's trusted roots;
- TLS with an explicit PEM CA bundle (replaces the system roots for that client);
- mutual TLS with a PEM client certificate chain and unencrypted PEM private key;
- SASL PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512, always over TLS.

SASL without TLS is rejected, including SCRAM. No mechanism fallback, custom
dialer, cloud authentication plugin or live credential refresh is introduced.
The existing broker-neutral Publisher and consumer Source interfaces are unchanged.

## Configuration contract

`SecurityConfig` contains `TLS TLSConfig` and `SASL SASLConfig`.

TLS fields:

- `Enabled`: enable TLS explicitly;
- `CAFile`: optional PEM trust bundle; absence uses system trust;
- `CertFile` and `KeyFile`: optional client certificate/key, supplied together;
- `ServerName`: optional certificate name override. Normally leave it empty so
  each seed and discovered broker is verified against its own address.

SASL fields:

- `Mechanism`: empty, `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`;
- `Username`: a nonempty identity;
- `Password` or `PasswordFile`: exactly one credential source.

Mechanism names are case-insensitive. TLS fields while TLS is disabled, unused
credentials without a mechanism, incomplete credentials and conflicting
password sources are errors. Passwords retain whitespace; password files may
end with one LF or CRLF, which is removed. Empty or NUL-containing credentials
are rejected. Secret files must be regular files, bounded in size and readable
by the application user. Symlinks to regular files are supported for mounted
secrets. Certificate files are limited to 1 MiB and password files to 16 KiB.

`Validate` checks structure without reading files or connecting. Client
construction reads and validates files before any Kafka connection. A consumer
factory snapshots its security material once; its workers use the same snapshot.
Replacing a file never changes an already-created client or factory silently.

## Standalone configuration

`emitlane run` and `emitlane doctor` use the same environment settings. `doctor`
does not connect to Kafka when configuration validation fails. A successful
connectivity check confirms reachability/authentication; it does not prove topic
or consumer-group permissions.

```sh
export EMITLANE_KAFKA_BROKERS=kafka.example.internal:9093
export EMITLANE_KAFKA_TLS_ENABLED=true
export EMITLANE_KAFKA_TLS_CA_FILE=/run/secrets/kafka-ca.pem
export EMITLANE_KAFKA_SASL_MECHANISM=SCRAM-SHA-512
export EMITLANE_KAFKA_SASL_USERNAME=emitlane
export EMITLANE_KAFKA_SASL_PASSWORD_FILE=/run/secrets/kafka-password
```

Keep the database and other required standalone settings configured as usual.
For mutual TLS, add `EMITLANE_KAFKA_TLS_CERT_FILE` and
`EMITLANE_KAFKA_TLS_KEY_FILE`. `EMITLANE_KAFKA_TLS_SERVER_NAME` overrides the
verified certificate name for every connection; normally omit it and provision
certificates covering all advertised broker addresses. If your broker uses a
publicly trusted certificate, omit `EMITLANE_KAFKA_TLS_CA_FILE` to use system roots.

An inline `EMITLANE_KAFKA_SASL_PASSWORD` is supported as an alternative to the
file. Do not put real credentials in a committed `.env` file or shell history.
Certificate/password rotation requires recreating clients; mounted-secret file
updates alone do not refresh existing clients.

## Go SDK

Use the same security value in `kafka.Config.Security` and
`kafka.ConsumerConfig.Security`:

```go
security := kafka.SecurityConfig{
    TLS: kafka.TLSConfig{Enabled: true, CAFile: "/run/secrets/kafka-ca.pem"},
    SASL: kafka.SASLConfig{
        Mechanism: kafka.SASLSCRAMSHA512,
        Username: "emitlane",
        PasswordFile: "/run/secrets/kafka-password",
    },
}
pub, err := kafka.NewPublisher(kafka.Config{
    Brokers: []string{"kafka.example.internal:9093"},
    PublishTimeout: 5 * time.Second,
    Security: security,
})
if err != nil {
    return err
}
defer pub.Close()
```

Applications using environment configuration can call
`config.LoadKafkaSecurity()` without requiring Relay or PostgreSQL settings.
The ecommerce example uses this loader for its managed consumer; its broker
addresses still come from the example's `KAFKA_BROKERS` variable. Configure the
same security variables independently in the Relay process/container.

## Failure and delivery behavior

Unreadable files, invalid PEM, mismatched client keys or invalid configuration
fail construction. An unavailable broker, failed certificate handshake, rejected
credentials or missing ACLs never trigger a plaintext fallback.

Before its first read, each consumer worker probes Kafka using the `Poll`
caller's context. This exposes startup authentication errors that the client's
background metadata retries would otherwise mask as a poll timeout. A failed
or canceled probe is retried on the next `Poll`; after success, polling adds no
further probes. This startup check does not prove topic/group ACLs or continuous
credential validity: later failures still follow the Kafka client's normal
fetch, group and commit error paths.

Network-time failures follow the existing publisher retry/dead policy. Security
does not change attempt accounting, leases, stream fencing, producer ACK policy,
producer retry settings, offset commits or the broker-ACK/SQL-ACK duplicate window.
No file reads or database transactions are added to publishing or handling.

Kafka authentication response details are not needed to identify a Kafka error
code. Adapter errors suppress broker-supplied details for typed Kafka errors;
other adapter error messages redact configured SASL credentials. Error identity
is retained where needed for retry classification and cancellation. Do not log
credentials explicitly or dump arbitrary application objects containing secrets.

## Rotation

1. Provision the replacement certificate/credentials and broker permissions.
2. Write replacement files atomically, readable only by the intended runtime.
3. Create and verify a new publisher or consumer factory. In standalone mode,
   restart Relay instances one at a time with the new configuration.
4. Drain the old clients using the normal shutdown procedure, then revoke the
   old credential after the rollout.

Existing connections keep their original credentials until recreated. Certificate
expiry and revoked credentials can cause delivery retries; monitor the backlog
and dead state and use audited retry after restoring access when necessary.

## Implementation and validation sequence

1. Shared TLS/SASL construction, validation and safe error formatting.
2. Publisher and consumer wiring; standalone environment settings and doctor.
3. Real Kafka tests for TLS, mutual TLS, PLAIN/SCRAM, wrong credentials, invalid
   trust/name/expiry, ACL rejection and credential rotation.
4. Existing Kafka outage, ambiguous publish, ordered handoff and managed offset
   recovery regressions on the resulting candidate.

The development suite includes bounded TLS handshakes, configuration/secret-file
checks and a real Kafka matrix under the `integration` build tag:

```sh
go test -count=1 -timeout=10m ./...
go test -tags=integration -count=1 -v -timeout=15m -run '^TestKafkaSecurityIntegration$' ./broker/kafka
```

Run these in the isolated test environment. The integration fixture uses one
`apache/kafka:4.3.1` container, temporary certificates and loopback-only exposed
ports. It checks TLS/mTLS, all three SASL mechanisms, publish/consume/commit and
group resume, wrong passwords, topic/group ACL rejection, invalid trust/name,
missing client certificate and password-file recreation. Expired certificates
are covered by the TLS handshake unit test. These tests do not establish live
broker credential-revocation or certificate-rotation behavior, multi-broker
failover, or a full release qualification. Authenticated integration tests have
been authored; passing results must be recorded from the candidate commit.

## Crash review

- Before client construction finishes: there is no publish and no state mutation.
- After construction but before publish: normal claim/lease recovery applies.
- During handshake or publish: the existing bounded call and recovery policy apply.
- After Kafka ACK but before SQL ACK: a duplicate remains possible.
- Concurrent Relay state transitions remain fenced by the existing SQL protocol.
- Failures are visible through startup errors, publish errors, retry/dead state,
  readiness and existing metrics. Configuration fixes/rotation are operator-driven;
  retry and lease recovery remain automatic within policy.

## References

- [franz-go TLS configuration](https://pkg.go.dev/github.com/twmb/franz-go@v1.21.6/pkg/kgo#DialTLSConfig)
- [Go TLS configuration](https://pkg.go.dev/crypto/tls#Config)
- [Kafka SASL authentication](https://kafka.apache.org/41/security/authentication-using-sasl/)
