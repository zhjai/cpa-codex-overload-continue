package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func configRequest(yaml string) []byte {
	raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte(yaml)})
	return raw
}

func TestConfigureRejectsLegacyContinuationFields(t *testing.T) {
	if err := configure(configRequest("enabled: true\nmodels: [gpt-6-astra]\ncontinuation_mode: text\n")); err == nil || !strings.Contains(err.Error(), "legacy field") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfigureRequiresModelAllowlistWhenEnabled(t *testing.T) {
	if err := configure(configRequest("enabled: true\npost_output_capacity: fail_closed\n")); err == nil || !strings.Contains(err.Error(), "models must be non-empty") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfigureDefaultsToFailClosedAndNoPluginRetry(t *testing.T) {
	if err := configure(configRequest("enabled: true\nmodels: [gpt-6-astra]\n")); err != nil {
		t.Fatal(err)
	}
	cfg := loadedConfig()
	if cfg.PostOutputCapacity != "fail_closed" || cfg.MaxPreOutputRetries != 0 {
		t.Fatalf("config = %#v", cfg)
	}
}
