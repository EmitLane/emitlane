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
	config.LeaseRenewInterval = config.LeaseDuration / 2
	if err := config.Validate(); err == nil {
		t.Fatal("expected unsafe renewal/lease configuration")
	}
	config = DefaultConfig()
	config.Consumer = "billing-v1"
	config.InstanceID = "billing-1"
	config.Jitter = 1.1
	if err := config.Validate(); err == nil {
		t.Fatal("expected invalid jitter")
	}
}
