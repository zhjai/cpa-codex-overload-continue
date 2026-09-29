package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
)

func configRequest(yaml string) []byte {
	raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte(yaml)})
	return raw
}

func TestConfigureRejectsLegacyContinuationFields(t *testing.T) {
	for _, field := range []string{"continuation_mode", "backoff_base_ms", "backoff_max_ms"} {
		if err := configure(configRequest("enabled: true\nmodels: [gpt-6-astra]\n" + field + ": 1\n")); err == nil || !strings.Contains(err.Error(), "legacy field") {
			t.Fatalf("field %s: error = %v", field, err)
		}
	}
}

func TestConfigureRejectsEnabledEmptyModelAllowlist(t *testing.T) {
	if err := configure(configRequest("enabled: true\npost_output_capacity: fail_closed\n")); err == nil || !strings.Contains(err.Error(), "models must be non-empty") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfigureRejectsPluginPreOutputRetries(t *testing.T) {
	if err := configure(configRequest("enabled: true\nmodels: [gpt-6-astra]\nmax_pre_output_retries: 1\n")); err == nil || !strings.Contains(err.Error(), "must be 0") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfigureRejectsUnknownFields(t *testing.T) {
	if err := configure(configRequest("enabled: true\nmodels: [gpt-6-astra]\npost_output_capcity: text_only\n")); err == nil || !strings.Contains(err.Error(), "field post_output_capcity not found") {
		t.Fatalf("error = %v", err)
	}
}

func TestConfigureAcceptsCPAInjectedPriority(t *testing.T) {
	for _, raw := range []string{
		"enabled: false\npriority: 0\n",
		"enabled: true\npriority: 17\nmodels: [gpt-6-astra]\n",
	} {
		if err := configure(configRequest(raw)); err != nil {
			t.Fatalf("CPA config %q rejected: %v", raw, err)
		}
	}
	if got := loadedConfig().Priority; got != 17 {
		t.Fatalf("priority = %d, want 17", got)
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

func TestCountTokensReturnsExplicitUnsupportedError(t *testing.T) {
	raw, err := handleMethod(pluginabi.MethodExecutorCountTokens, nil)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	if env.OK || env.Error == nil || env.Error.Code != "unsupported_operation" || env.Error.HTTPStatus != http.StatusNotImplemented {
		t.Fatalf("count-token response = %#v", env)
	}
}
