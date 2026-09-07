package consumer

import "testing"

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	if err := config.Validate(); err == nil {
		t.Fatal("expected missing consumer name")
	}
	config.Consumer = "billing-v1"
	if err := config.Validate(); err == nil {
		t.Fatal("expected missing instance ID")
	}
	config.InstanceID = "billing-v1"
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	config.LeaseDuration = config.HandlerTimeout
	if err := config.Validate(); err == nil {
		t.Fatal("expected unsafe handler/lease configuration")
	}
}
