package main

import (
	"encoding/json"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestRouteModelOnlyMatchesEnabledOpenAIResponsesCodex(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", MaxPreCommitRetries: 2, ContinuationMode: "text"})
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
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex"})
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
