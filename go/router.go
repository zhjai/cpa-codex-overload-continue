package main

import (
	"encoding/json"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func routeModel(raw []byte) ([]byte, error) {
	var req pluginapi.ModelRouteRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	cfg := loadedConfig()
	if !cfg.Enabled || !sourceFormatMatches(req.SourceFormat) || !modelAllowed(req.RequestedModel, cfg.Models) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	if !providerAvailable(req.AvailableProviders, cfg.Provider) {
		return okEnvelope(pluginapi.ModelRouteResponse{Handled: false})
	}
	return okEnvelope(pluginapi.ModelRouteResponse{Handled: true, TargetKind: pluginapi.ModelRouteTargetSelf, Reason: "codex_server_overload_recovery"})
}

func sourceFormatMatches(format string) bool {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "openai-response", "openai_responses", "responses":
		return true
	default:
		return false
	}
}

func providerAvailable(providers []string, wanted string) bool {
	wanted = strings.ToLower(strings.TrimSpace(wanted))
	for _, provider := range providers {
		if strings.ToLower(strings.TrimSpace(provider)) == wanted {
			return true
		}
	}
	return false
}

func modelAllowed(model string, allowlist []string) bool {
	if len(allowlist) == 0 {
		return true
	}
	model = strings.TrimSpace(model)
	for _, allowed := range allowlist {
		allowed = strings.TrimSpace(allowed)
		if allowed == "*" || allowed == model {
			return true
		}
	}
	return false
}
