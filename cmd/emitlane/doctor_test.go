package main

import (
	"io"
	"net"
	"os"
	"strings"
	"testing"
)

// A malformed security configuration must never lead doctor to probe a broker
// with plaintext defaults. The local listener records any attempted connection.
func TestDoctorDoesNotDialKafkaWithInvalidSecurity(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	connected := make(chan bool, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			_ = conn.Close()
		}
		connected <- err == nil
	}()
	t.Setenv("EMITLANE_DATABASE_URL", "")
	t.Setenv("EMITLANE_KAFKA_BROKERS", listener.Addr().String())
	t.Setenv("EMITLANE_KAFKA_TLS_ENABLED", "false")
	t.Setenv("EMITLANE_KAFKA_TLS_CA_FILE", "/private/ca.pem")
	output, err := os.CreateTemp(t.TempDir(), "doctor")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	previous := os.Stdout
	os.Stdout = output
	t.Cleanup(func() { os.Stdout = previous })
	err = doctorCmd(nil)
	os.Stdout = previous
	_ = listener.Close()
	if <-connected {
		t.Fatal("doctor dialed Kafka despite invalid security configuration")
	}
	if err == nil {
		t.Fatal("doctor succeeded with invalid security configuration")
	}
	if _, err := output.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "fix configuration before probing Kafka") || strings.Contains(string(data), "/private/ca.pem") {
		t.Fatal("missing safe Kafka diagnostic")
	}
}
