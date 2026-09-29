package main

import (
	"encoding/json"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestRouteModelOnlyMatchesEnabledOpenAIResponsesCodex(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"gpt-5.5"}, PostOutputCapacity: "fail_closed"})
	raw, _ := json.Marshal(pluginapi.ModelRouteRequest{SourceFormat: "openai-response", RequestedModel: "gpt-5.5", AvailableProviders: []string{"codex"}})
	got, err := routeModel(raw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(got, &env); err != nil {
		t.Fatal(err)
	}
	var decision pluginapi.ModelRouteResponse
	if err := json.Unmarshal(env.Result, &decision); err != nil {
		t.Fatal(err)
	}
	if !decision.Handled || decision.TargetKind != pluginapi.ModelRouteTargetSelf {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestRouteModelDeclinesOtherSourceOrProvider(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"*"}})
	for _, req := range []pluginapi.ModelRouteRequest{{SourceFormat: "openai", RequestedModel: "gpt-5.5", AvailableProviders: []string{"codex"}}, {SourceFormat: "openai-response", RequestedModel: "gpt-5.5", AvailableProviders: []string{"openai"}}} {
		raw, _ := json.Marshal(req)
		out, err := routeModel(raw)
		if err != nil {
			t.Fatal(err)
		}
		var env envelope
		_ = json.Unmarshal(out, &env)
		var decision pluginapi.ModelRouteResponse
		_ = json.Unmarshal(env.Result, &decision)
		if decision.Handled {
			t.Fatalf("unexpected handled decision for %#v", req)
		}
	}
}

func TestRouteModelEmptyAllowlistDoesNotMatchEnabledConfig(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex"})
	if err := configure(configRequest("enabled: true\n")); err == nil {
		t.Fatal("enabled empty allowlist should be rejected")
	}
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex"})
	raw, _ := json.Marshal(pluginapi.ModelRouteRequest{SourceFormat: "openai-response", RequestedModel: "gpt-6-astra", AvailableProviders: []string{"codex"}})
	result, err := routeModel(raw)
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(result, &env); err != nil {
		t.Fatal(err)
	}
	var decision pluginapi.ModelRouteResponse
	if err := json.Unmarshal(env.Result, &decision); err != nil {
		t.Fatal(err)
	}
	if decision.Handled {
		t.Fatalf("empty allowlist must not route: %#v", decision)
	}
}
