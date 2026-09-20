package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	maxTLSFileBytes    = 1024 * 1024
	maxCredentialBytes = 16 * 1024
)

// SASLMechanism identifies one supported Kafka authentication mechanism.
type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "PLAIN"
	SASLSCRAMSHA256 SASLMechanism = "SCRAM-SHA-256"
	SASLSCRAMSHA512 SASLMechanism = "SCRAM-SHA-512"
)

// SecurityConfig is shared by the Kafka publisher and consumer factory.
// The zero value preserves plaintext connections without authentication.
type SecurityConfig struct {
	TLS  TLSConfig
	SASL SASLConfig
}

// TLSConfig enables verified TLS. Files are read once during construction.
// CAFile replaces system trust with the supplied PEM bundle. CertFile and
// KeyFile enable mutual TLS and must be specified together.
type TLSConfig struct {
	Enabled    bool
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
}

// SASLConfig enables authentication over TLS. Supply Password or PasswordFile,
// never both. Recreate the client/factory to rotate credentials. Credentials
// are omitted from JSON and ordinary formatted/structured logging.
type SASLConfig struct {
	Mechanism    SASLMechanism
	Username     string `json:"-" yaml:"-"`
	Password     string `json:"-" yaml:"-"`
	PasswordFile string `json:"-" yaml:"-"`
}

func (c SASLConfig) mechanism() SASLMechanism {
	return SASLMechanism(strings.ToUpper(strings.TrimSpace(string(c.Mechanism))))
}

// String returns a representation without credential values or file paths.
func (c SASLConfig) String() string { return "SASLConfig{credentials:[REDACTED]}" }

// GoString also protects credentials in %#v formatting.
func (c SASLConfig) GoString() string { return c.String() }

// LogValue protects credentials when the config is attached to a slog record.
func (c SASLConfig) LogValue() slog.Value { return slog.StringValue(c.String()) }

// Validate checks configuration structure without file reads or network I/O.
func (c SecurityConfig) Validate() error {
	if !c.TLS.Enabled && (c.TLS.CAFile != "" || c.TLS.CertFile != "" || c.TLS.KeyFile != "" || c.TLS.ServerName != "") {
		return errors.New("kafka: TLS settings require TLS to be enabled")
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		return errors.New("kafka: TLS client certificate and key must be supplied together")
	}
	if c.mechanismDisabled() {
		if c.SASL.Username != "" || c.SASL.Password != "" || c.SASL.PasswordFile != "" {
			return errors.New("kafka: SASL credentials require a mechanism")
		}
		return nil
	}
	switch c.SASL.mechanism() {
	case SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512:
	default:
		return errors.New("kafka: SASL mechanism must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512")
	}
	if !c.TLS.Enabled {
		return errors.New("kafka: SASL requires TLS")
	}
	if strings.TrimSpace(c.SASL.Username) == "" || !validCredential(c.SASL.Username) {
		return errors.New("kafka: SASL username must be nonempty UTF-8 without NUL and at most 16 KiB")
	}
	if c.SASL.Password != "" && c.SASL.PasswordFile != "" {
		return errors.New("kafka: SASL password and password file are mutually exclusive")
	}
	if c.SASL.Password == "" && c.SASL.PasswordFile == "" {
		return errors.New("kafka: SASL password or password file is required")
	}
	if c.SASL.Password != "" && !validCredential(c.SASL.Password) {
		return errors.New("kafka: SASL password must be nonempty UTF-8 without NUL and at most 16 KiB")
	}
	return nil
}

func (c SecurityConfig) mechanismDisabled() bool { return c.SASL.mechanism() == "" }

func validCredential(value string) bool {
	return len(value) > 0 && len(value) <= maxCredentialBytes && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

// readSecurityFile avoids unbounded allocations and excludes paths and file
// contents from returned errors. Stat before Open also rejects ordinary FIFOs
// without blocking. Configuration paths must be controlled by the operator.
func readSecurityFile(path, kind string, limit int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("kafka: %s must be a readable regular file", kind)
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("kafka: %s exceeds size limit", kind)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot read %s", kind)
	}
	defer func() { _ = file.Close() }()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("kafka: %s must be a readable regular file", kind)
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("kafka: cannot read %s", kind)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("kafka: %s exceeds size limit", kind)
	}
	return data, nil
}

func (c TLSConfig) load() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.ServerName}
	if c.CAFile != "" {
		data, err := readSecurityFile(c.CAFile, "TLS CA file", maxTLSFileBytes)
		if err != nil {
			return nil, err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			return nil, errors.New("kafka: TLS CA file contains no valid PEM certificates")
		}
		config.RootCAs = roots
	}
	if c.CertFile != "" {
		certPEM, err := readSecurityFile(c.CertFile, "TLS client certificate file", maxTLSFileBytes)
		if err != nil {
			return nil, err
		}
		keyPEM, err := readSecurityFile(c.KeyFile, "TLS client key file", maxTLSFileBytes)
		if err != nil {
			return nil, err
		}
		certificate, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, errors.New("kafka: invalid TLS client certificate/key pair")
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

type clientSecurity struct {
	options []kgo.Opt
	redact  func(string) string
}

func newClientSecurity(config SecurityConfig) (clientSecurity, error) {
	var security clientSecurity
	if err := config.Validate(); err != nil {
		return security, err
	}
	tlsConfig, err := config.TLS.load()
	if err != nil {
		return security, err
	}
	if tlsConfig != nil {
		security.options = append(security.options, kgo.DialTLSConfig(tlsConfig))
	}
	if config.mechanismDisabled() {
		return security, nil
	}
	password := config.SASL.Password
	if config.SASL.PasswordFile != "" {
		data, err := readSecurityFile(config.SASL.PasswordFile, "SASL password file", maxCredentialBytes)
		if err != nil {
			return clientSecurity{}, err
		}
		password = strings.TrimSuffix(string(data), "\n")
		if len(data) > 0 && data[len(data)-1] == '\n' {
			password = strings.TrimSuffix(password, "\r")
		}
	}
	if !validCredential(password) {
		return clientSecurity{}, errors.New("kafka: SASL password must be nonempty UTF-8 without NUL and at most 16 KiB")
	}
	var mechanism sasl.Mechanism
	switch config.SASL.mechanism() {
	case SASLPlain:
		mechanism = plain.Auth{User: config.SASL.Username, Pass: password}.AsMechanism()
	case SASLSCRAMSHA256:
		mechanism = scram.Auth{User: config.SASL.Username, Pass: password}.AsSha256Mechanism()
	case SASLSCRAMSHA512:
		mechanism = scram.Auth{User: config.SASL.Username, Pass: password}.AsSha512Mechanism()
	}
	security.options = append(security.options, kgo.SASL(mechanism))
	security.redact = credentialRedactor(password, config.SASL.Username)
	return security, nil
}

func credentialRedactor(first, second string) func(string) string {
	// Match longer overlapping credentials first. Keep the values inside a
	// closure so formatting a client does not expose a raw credential slice.
	if len(second) > len(first) {
		first, second = second, first
	}
	replacer := strings.NewReplacer(first, "[REDACTED]", second, "[REDACTED]")
	return replacer.Replace
}

func (s clientSecurity) safeError(err error) error {
	if err == nil {
		return nil
	}
	var kafkaError *kerr.Error
	if errors.As(err, &kafkaError) {
		// The code is enough for retry classification; broker-supplied SASL
		// error details may contain authentication identities or credentials.
		if known := kerr.ErrorForCode(kafkaError.Code); known != nil {
			return known
		}
	}
	message := err.Error()
	if s.redact != nil {
		message = s.redact(message)
	}
	if message == err.Error() {
		return err
	}
	return &redactedError{cause: err, message: message}
}

type redactedError struct {
	cause   error
	message string
}

func (e *redactedError) Error() string    { return e.message }
func (e *redactedError) GoString() string { return e.message }
func (e *redactedError) Unwrap() error    { return e.cause }
