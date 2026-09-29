package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct { void* ptr; size_t len; } cliproxy_buffer;
typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);
typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;
typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);
typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

// The declarations below are provided by cgo for the //export functions.
extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static void set_plugin_callbacks(cliproxy_plugin_api* plugin) {
	plugin->call = cliproxyPluginCall;
	plugin->free_buffer = cliproxyPluginFree;
	plugin->shutdown = cliproxyPluginShutdown;
}

static const cliproxy_host_api* stored_host;
static void store_host_api(const cliproxy_host_api* host) { stored_host = host; }
static int call_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) return 1;
	return stored_host->call(stored_host->host_ctx, method, request, request_len, response);
}
static void free_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) stored_host->free_buffer(ptr, len);
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

const pluginIdentifier = "codex-overload-continue"

type envelope struct {
	OK     bool             `json:"ok"`
	Result json.RawMessage  `json:"result,omitempty"`
	Error  *pluginabi.Error `json:"error,omitempty"`
}

type registration struct {
	SchemaVersion uint32             `json:"schema_version"`
	Metadata      pluginapi.Metadata `json:"metadata"`
	Capabilities  registrationCaps   `json:"capabilities"`
}

type registrationCaps struct {
	ModelRouter           bool     `json:"model_router"`
	Executor              bool     `json:"executor"`
	ExecutorModelScope    string   `json:"executor_model_scope"`
	ExecutorInputFormats  []string `json:"executor_input_formats"`
	ExecutorOutputFormats []string `json:"executor_output_formats"`
}

type lifecycleRequest struct {
	ConfigYAML []byte `json:"config_yaml"`
}

// pluginConfig deliberately requires an explicit allowlist in production. An empty
// allowlist means all openai-response models, which is useful for a local test only.
type pluginConfig struct {
	Enabled             bool     `yaml:"enabled"`
	Provider            string   `yaml:"provider"`
	Models              []string `yaml:"models"`
	MaxPreCommitRetries int      `yaml:"max_pre_commit_retries"`
	MaxContinuations    int      `yaml:"max_continuations"`
	BackoffBaseMS       int      `yaml:"backoff_base_ms"`
	BackoffMaxMS        int      `yaml:"backoff_max_ms"`
	ContinuationMode    string   `yaml:"continuation_mode"`
	ContinuePrompt      string   `yaml:"continue_prompt"`
	RewriteResponseID   bool     `yaml:"rewrite_response_id"`
}

var currentConfig atomic.Value

type rpcExecutorRequest struct {
	pluginapi.ExecutorRequest
	StreamID       string `json:"stream_id,omitempty"`
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcStreamEmitRequest struct {
	StreamID string `json:"stream_id"`
	Payload  []byte `json:"payload,omitempty"`
	Error    string `json:"error,omitempty"`
}

type rpcStreamCloseRequest struct {
	StreamID string `json:"stream_id"`
	Error    string `json:"error,omitempty"`
}

type hostModelExecutionRequest struct {
	pluginapi.HostModelExecutionRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type hostRPCError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *hostRPCError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

func defaultConfig() pluginConfig {
	return pluginConfig{
		Enabled:             false,
		Provider:            "codex",
		MaxPreCommitRetries: 2,
		MaxContinuations:    1,
		BackoffBaseMS:       250,
		BackoffMaxMS:        4000,
		ContinuationMode:    "disabled",
		ContinuePrompt:      "Continue exactly where you stopped. Do not repeat text already produced; finish the response.",
		RewriteResponseID:   true,
	}
}

func loadedConfig() pluginConfig {
	if raw := currentConfig.Load(); raw != nil {
		if cfg, ok := raw.(pluginConfig); ok {
			return cfg
		}
	}
	return defaultConfig()
}

func configure(raw []byte) error {
	cfg := defaultConfig()
	if len(raw) > 0 {
		var req lifecycleRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return err
		}
		if len(req.ConfigYAML) > 0 {
			if err := yaml.Unmarshal(req.ConfigYAML, &cfg); err != nil {
				return err
			}
		}
	}
	if cfg.Provider == "" {
		cfg.Provider = "codex"
	}
	cfg.Provider = strings.ToLower(strings.TrimSpace(cfg.Provider))
	cfg.ContinuationMode = strings.ToLower(strings.TrimSpace(cfg.ContinuationMode))
	if cfg.ContinuationMode == "" {
		cfg.ContinuationMode = "text"
	}
	if cfg.ContinuationMode != "disabled" && cfg.ContinuationMode != "text" {
		return fmt.Errorf("continuation_mode must be disabled or text")
	}
	if cfg.MaxPreCommitRetries < 0 || cfg.MaxPreCommitRetries > 5 {
		return fmt.Errorf("max_pre_commit_retries must be between 0 and 5")
	}
	if cfg.MaxContinuations < 0 || cfg.MaxContinuations > 1 {
		return fmt.Errorf("max_continuations must be 0 or 1")
	}
	if cfg.BackoffBaseMS < 0 || cfg.BackoffMaxMS < cfg.BackoffBaseMS {
		return fmt.Errorf("invalid backoff bounds")
	}
	if cfg.BackoffMaxMS == 0 {
		cfg.BackoffMaxMS = 4000
	}
	currentConfig.Store(cfg)
	return nil
}

func pluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name: pluginIdentifier, Version: "0.1.0", Author: "zhjai",
			GitHubRepository: "https://github.com/router-for-me/CLIProxyAPI",
			ConfigFields: []pluginapi.ConfigField{
				{Name: "enabled", Type: pluginapi.ConfigFieldTypeBoolean, Description: "Enable the opt-in Codex overload recovery router."},
				{Name: "provider", Type: pluginapi.ConfigFieldTypeString, Description: "Provider forced for nested execution; normally codex."},
				{Name: "models", Type: pluginapi.ConfigFieldTypeArray, Description: "Model allowlist. Empty matches every openai-response model."},
				{Name: "max_pre_commit_retries", Type: pluginapi.ConfigFieldTypeInteger, Description: "Retries when overload happens before generated output is committed."},
				{Name: "max_continuations", Type: pluginapi.ConfigFieldTypeInteger, Description: "At most one text-only continuation is supported."},
				{Name: "backoff_base_ms", Type: pluginapi.ConfigFieldTypeInteger, Description: "Initial retry backoff in milliseconds."},
				{Name: "backoff_max_ms", Type: pluginapi.ConfigFieldTypeInteger, Description: "Maximum retry backoff in milliseconds."},
				{Name: "continuation_mode", Type: pluginapi.ConfigFieldTypeEnum, EnumValues: []string{"disabled", "text"}, Description: "Text-only continuation; tool-call turns always fail closed."},
				{Name: "continue_prompt", Type: pluginapi.ConfigFieldTypeString, Description: "Prompt appended to a safe text-only continuation."},
				{Name: "rewrite_response_id", Type: pluginapi.ConfigFieldTypeBoolean, Description: "Reuse the first response id on continuation events when possible."},
			},
		},
		Capabilities: registrationCaps{
			ModelRouter: true, Executor: true,
			ExecutorModelScope:    string(pluginapi.ExecutorModelScopeStatic),
			ExecutorInputFormats:  []string{"openai-response"},
			ExecutorOutputFormats: []string{"openai-response"},
		},
	}
}

func okEnvelope(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(pluginabi.Envelope{OK: true, Result: raw})
}

func errorEnvelope(code, message string, status int) []byte {
	raw, err := pluginabi.NewErrorEnvelope(code, message, status)
	if err != nil {
		return []byte(`{"ok":false,"error":{"code":"plugin_error","message":"serialization failed"}}`)
	}
	return raw
}

func handleMethod(method string, raw []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		if err := configure(raw); err != nil {
			return nil, err
		}
		return okEnvelope(pluginRegistration())
	case pluginabi.MethodModelRoute:
		return routeModel(raw)
	case pluginabi.MethodExecutorIdentifier:
		return okEnvelope(map[string]string{"identifier": pluginIdentifier})
	case pluginabi.MethodExecutorExecute:
		return execute(raw)
	case pluginabi.MethodExecutorExecuteStream:
		return executeStream(raw)
	case pluginabi.MethodExecutorCountTokens:
		return okEnvelope(pluginapi.ExecutorResponse{Payload: []byte(`{"input_tokens":0}`)})
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method, http.StatusInternalServerError), nil
	}
}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	C.store_host_api(host)
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	C.set_plugin_callbacks(plugin)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required", 500))
		return 1
	}
	var raw []byte
	if request != nil && requestLen > 0 {
		raw = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	out, err := handleMethod(C.GoString(method), raw)
	if err != nil {
		writeResponse(response, errorEnvelope("plugin_error", err.Error(), 500))
		return 1
	}
	writeResponse(response, out)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil {
		return
	}
	if len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}

func callHost(method string, payload any) (json.RawMessage, error) {
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal host callback %s: %w", method, err)
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	var requestPtr *C.uint8_t
	if len(rawPayload) > 0 {
		ptr := C.CBytes(rawPayload)
		if ptr == nil {
			return nil, fmt.Errorf("allocate host callback %s", method)
		}
		defer C.free(ptr)
		requestPtr = (*C.uint8_t)(ptr)
	}
	var response C.cliproxy_buffer
	code := C.call_host_api(cMethod, requestPtr, C.size_t(len(rawPayload)), &response)
	var rawResponse []byte
	if response.ptr != nil && response.len > 0 {
		rawResponse = C.GoBytes(response.ptr, C.int(response.len))
	}
	if response.ptr != nil {
		C.free_host_buffer(response.ptr, response.len)
	}
	if len(rawResponse) == 0 {
		return nil, fmt.Errorf("host callback %s returned no response, code=%d", method, int(code))
	}
	var env pluginabi.Envelope
	if err := json.Unmarshal(rawResponse, &env); err != nil {
		return nil, fmt.Errorf("decode host callback %s: %w", method, err)
	}
	if !env.OK {
		if env.Error == nil {
			return nil, fmt.Errorf("host callback %s failed", method)
		}
		return nil, &hostRPCError{Code: env.Error.Code, Message: env.Error.Message, HTTPStatus: env.Error.HTTPStatus}
	}
	return env.Result, nil
}

func emitPluginStreamChunk(streamID string, payload []byte) error {
	if strings.TrimSpace(streamID) == "" {
		return fmt.Errorf("stream id is required")
	}
	_, err := callHost(pluginabi.MethodHostStreamEmit, rpcStreamEmitRequest{StreamID: streamID, Payload: payload})
	return err
}

func closePluginStream(streamID, msg string) {
	if strings.TrimSpace(streamID) == "" {
		return
	}
	_, _ = callHost(pluginabi.MethodHostStreamClose, rpcStreamCloseRequest{StreamID: streamID, Error: strings.TrimSpace(msg)})
}

func hostModelRequest(req pluginapi.ExecutorRequest, body []byte, callbackID string) hostModelExecutionRequest {
	return hostModelExecutionRequest{
		HostModelExecutionRequest: pluginapi.HostModelExecutionRequest{
			EntryProtocol: "openai-response", ExitProtocol: "openai-response", Model: req.Model,
			Stream: true, Body: body, Headers: req.Headers, Query: req.Query,
			ForcedProvider: loadedConfig().Provider, AuthID: req.AuthID,
		},
		HostCallbackID: callbackID,
	}
}

func requestBody(req pluginapi.ExecutorRequest) []byte {
	if len(req.OriginalRequest) > 0 {
		return append([]byte(nil), req.OriginalRequest...)
	}
	return append([]byte(nil), req.Payload...)
}

func main() {}
