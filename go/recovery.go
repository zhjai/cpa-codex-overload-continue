package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type sseFrame struct {
	Raw       []byte
	JSON      map[string]any
	EventType string
}

type sseDecoder struct{ buffer []byte }

func (d *sseDecoder) feed(payload []byte, final bool) []sseFrame {
	d.buffer = append(d.buffer, payload...)
	var out []sseFrame
	for {
		idx, sepLen := bytes.Index(d.buffer, []byte("\n\n")), 2
		if idx < 0 {
			idx, sepLen = bytes.Index(d.buffer, []byte("\r\n\r\n")), 4
		}
		if idx < 0 {
			break
		}
		raw := append([]byte(nil), d.buffer[:idx+sepLen]...)
		d.buffer = d.buffer[idx+sepLen:]
		if frame, ok := decodeSSEFrame(raw); ok {
			out = append(out, frame)
		}
	}
	if final && len(bytes.TrimSpace(d.buffer)) > 0 {
		if frame, ok := decodeSSEFrame(append(append([]byte(nil), d.buffer...), '\n', '\n')); ok {
			out = append(out, frame)
		}
		d.buffer = nil
	}
	return out
}

type attemptState struct {
	Generated      bool
	ToolCall       bool
	Forwarded      bool
	SemanticOutput bool
	Overloaded     bool
}

type streamAttemptResult struct {
	State          attemptState
	Status         int
	Err            error
	ClosePayload   string
	StopAfterFrame bool
}

// streamFailure carries a structured payload to host.stream.close. A free-form
// error string makes CPA synthesize a generic error and can accidentally turn a
// tool-side failure into a client retry.
type streamFailure struct{ message, payload string }

func (e *streamFailure) Error() string { return e.message }

func execute(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	cfg := loadedConfig()
	body := requestBody(req.ExecutorRequest)
	if len(body) == 0 {
		return errorEnvelope("invalid_request", "empty Responses request", http.StatusBadRequest), nil
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		resp, err := hostModelExecute(req.ExecutorRequest, body, req.HostCallbackID)
		if err == nil && resp.StatusCode < 400 {
			return okEnvelope(pluginapi.ExecutorResponse{Payload: resp.Body, Headers: resp.Headers})
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("upstream status %d", resp.StatusCode)
		}
		if isOverloadError(resp.Body, resp.StatusCode, err) && attempt < cfg.MaxPreOutputRetries {
			if waitErr := waitBackoff(context.Background(), attempt, cfg); waitErr != nil {
				lastErr = waitErr
				break
			}
			continue
		}
		if isOverloadError(resp.Body, resp.StatusCode, err) {
			return errorEnvelope("server_is_overloaded", lastErr.Error(), http.StatusServiceUnavailable), nil
		}
		return errorEnvelope("upstream_error", lastErr.Error(), hostErrorStatus(err)), nil
	}
	return errorEnvelope("upstream_error", lastErr.Error(), hostErrorStatus(lastErr)), nil
}

func executeStream(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.StreamID) == "" {
		return errorEnvelope("invalid_request", "stream_id is required", http.StatusBadRequest), nil
	}
	go func() {
		if err := runStream(req); err != nil {
			if failure, ok := err.(*streamFailure); ok && failure.payload != "" {
				closePluginStream(req.StreamID, failure.payload)
				return
			}
			closePluginStream(req.StreamID, originalFailurePayload(err.Error()))
			return
		}
		closePluginStream(req.StreamID, "")
	}()
	return okEnvelope(map[string]any{"headers": map[string][]string{"Content-Type": {"text/event-stream"}}})
}

func runStream(req rpcExecutorRequest) error {
	cfg := loadedConfig()
	body := requestBody(req.ExecutorRequest)
	if len(body) == 0 {
		return &streamFailure{message: "empty Responses request", payload: canonicalUpstreamError("empty Responses request")}
	}
	for attempt := 0; ; attempt++ {
		result := runStreamAttempt(req.ExecutorRequest, body, req.HostCallbackID, req.StreamID, cfg)
		if result.Err == nil {
			return nil
		}
		if result.StopAfterFrame {
			return nil
		}
		if result.ClosePayload != "" {
			return &streamFailure{message: result.Err.Error(), payload: result.ClosePayload}
		}
		if !result.State.Overloaded {
			return result.Err
		}
		if !result.State.SemanticOutput && !result.State.ToolCall && !result.State.Forwarded && attempt < cfg.MaxPreOutputRetries {
			if err := waitBackoff(context.Background(), attempt, cfg); err != nil {
				return err
			}
			continue
		}
		return &streamFailure{message: result.Err.Error(), payload: originalFailurePayload(result.Err.Error())}
	}
}

func runStreamAttempt(req pluginapi.ExecutorRequest, body []byte, callbackID, pluginStreamID string, cfg pluginConfig) streamAttemptResult {
	state := attemptState{}
	resp, err := hostModelExecuteStream(req, body, callbackID)
	if err != nil {
		state.Overloaded = isOverloadError(nil, hostErrorStatus(err), err)
		payload := closePayloadForError(err.Error(), state, cfg)
		if state.Overloaded && !streamCommitted(state) {
			payload = ""
		}
		return streamAttemptResult{State: state, Status: hostErrorStatus(err), Err: err, ClosePayload: payload}
	}
	if resp.StatusCode >= 400 {
		state.Overloaded = isOverloadError(nil, resp.StatusCode, nil)
		err := fmt.Errorf("upstream status %d", resp.StatusCode)
		payload := closePayloadForError(err.Error(), state, cfg)
		if state.Overloaded && !streamCommitted(state) {
			payload = ""
		}
		return streamAttemptResult{State: state, Status: resp.StatusCode, Err: err, ClosePayload: payload}
	}
	if resp.StreamID == "" {
		return streamAttemptResult{State: state, Err: fmt.Errorf("host returned empty stream id")}
	}
	defer closeHostModelStream(resp.StreamID)
	decoder := &sseDecoder{}
	for {
		chunk, errRead := readHostModelStream(resp.StreamID)
		if errRead != nil {
			state.Overloaded = isOverloadError(nil, hostErrorStatus(errRead), errRead)
			payload := closePayloadForError(errRead.Error(), state, cfg)
			if state.Overloaded && !streamCommitted(state) {
				payload = ""
			}
			return streamAttemptResult{State: state, Status: hostErrorStatus(errRead), Err: errRead, ClosePayload: payload}
		}
		if chunk.Error != "" {
			return streamErrorResult(state, chunk.Error, cfg)
		}
		if len(chunk.Payload) > 0 {
			for _, frame := range decoder.feed(chunk.Payload, false) {
				result, stop := forwardFrame(pluginStreamID, frame, &state, cfg)
				if result.Err != nil || stop {
					return result
				}
			}
		}
		if chunk.Done {
			for _, frame := range decoder.feed(nil, true) {
				result, stop := forwardFrame(pluginStreamID, frame, &state, cfg)
				if result.Err != nil || stop {
					return result
				}
			}
			return streamAttemptResult{State: state, Status: http.StatusOK}
		}
	}
}

func forwardFrame(pluginStreamID string, frame sseFrame, state *attemptState, cfg pluginConfig) (streamAttemptResult, bool) {
	if frameIsOverload(frame) {
		state.Overloaded = true
		if canRewriteCapacity(*state, cfg) {
			if err := emitPluginStreamChunk(pluginStreamID, rewriteCapacitySSEFrame(frame.Raw)); err != nil {
				return streamAttemptResult{State: *state, Err: err}, false
			}
			state.Forwarded = true
			return streamAttemptResult{State: *state, Err: fmt.Errorf("capacity error rewritten as server_error"), StopAfterFrame: true}, true
		}
		return streamAttemptResult{State: *state, Err: fmt.Errorf("server_is_overloaded")}, false
	}
	observeFrame(state, frame)
	if err := emitPluginStreamChunk(pluginStreamID, frame.Raw); err != nil {
		return streamAttemptResult{State: *state, Err: err}, false
	}
	state.Forwarded = true
	return streamAttemptResult{State: *state}, false
}

func streamErrorResult(state attemptState, raw string, cfg pluginConfig) streamAttemptResult {
	state.Overloaded = isOverloadError(nil, 0, fmt.Errorf("%s", raw))
	if state.Overloaded && !streamCommitted(state) {
		return streamAttemptResult{State: state, Err: fmt.Errorf("%s", raw)}
	}
	return streamAttemptResult{State: state, Err: fmt.Errorf("%s", raw), ClosePayload: closePayloadForError(raw, state, cfg)}
}

func streamCommitted(state attemptState) bool {
	return state.Forwarded || state.SemanticOutput || state.ToolCall
}

func canRewriteCapacity(state attemptState, cfg pluginConfig) bool {
	return cfg.PostOutputCapacity == "text_only" && state.SemanticOutput && !state.ToolCall
}

func closePayloadForError(raw string, state attemptState, cfg pluginConfig) string {
	if isOverloadError(nil, 0, fmt.Errorf("%s", raw)) && canRewriteCapacity(state, cfg) {
		return normalizedServerErrorPayload(raw)
	}
	return originalFailurePayload(raw)
}

func originalFailurePayload(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return canonicalUpstreamError("upstream stream failed")
	}
	var value any
	if json.Unmarshal([]byte(trimmed), &value) == nil {
		return trimmed
	}
	return canonicalUpstreamError(trimmed)
}

func canonicalUpstreamError(message string) string {
	return marshalErrorPayload("upstream_error", "upstream_error", message)
}
func normalizedServerErrorPayload(message string) string {
	return marshalErrorPayload("server_error", "server_error", errorMessage(message))
}
func marshalErrorPayload(typ, code, message string) string {
	raw, _ := json.Marshal(map[string]any{"error": map[string]any{"type": typ, "code": code, "message": message}})
	return string(raw)
}
func errorMessage(raw string) string {
	var value any
	if json.Unmarshal([]byte(strings.TrimSpace(raw)), &value) == nil {
		if msg := findMessage(value); msg != "" {
			return msg
		}
	}
	return strings.TrimSpace(raw)
}

func hostModelExecute(req pluginapi.ExecutorRequest, body []byte, callbackID string) (pluginapi.HostModelExecutionResponse, error) {
	raw, err := callHost(pluginabi.MethodHostModelExecute, hostModelExecutionRequest{HostModelExecutionRequest: pluginapi.HostModelExecutionRequest{EntryProtocol: "openai-response", ExitProtocol: "openai-response", Model: req.Model, Stream: false, Body: body, Headers: req.Headers, Query: req.Query, ForcedProvider: loadedConfig().Provider, AuthID: req.AuthID}, HostCallbackID: callbackID})
	if err != nil {
		return pluginapi.HostModelExecutionResponse{}, err
	}
	var resp pluginapi.HostModelExecutionResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}
func hostModelExecuteStream(req pluginapi.ExecutorRequest, body []byte, callbackID string) (pluginapi.HostModelStreamResponse, error) {
	raw, err := callHost(pluginabi.MethodHostModelExecuteStream, hostModelRequest(req, body, callbackID))
	if err != nil {
		return pluginapi.HostModelStreamResponse{}, err
	}
	var resp pluginapi.HostModelStreamResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}
func readHostModelStream(id string) (pluginapi.HostModelStreamReadResponse, error) {
	raw, err := callHost(pluginabi.MethodHostModelStreamRead, pluginapi.HostModelStreamReadRequest{StreamID: id})
	if err != nil {
		return pluginapi.HostModelStreamReadResponse{}, err
	}
	var resp pluginapi.HostModelStreamReadResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}
func closeHostModelStream(id string) {
	_, _ = callHost(pluginabi.MethodHostModelStreamClose, pluginapi.HostModelStreamCloseRequest{StreamID: id})
}
func hostErrorStatus(err error) int {
	if e, ok := err.(*hostRPCError); ok {
		return e.HTTPStatus
	}
	return 0
}

func isOverloadError(body []byte, status int, err error) bool {
	if (status == http.StatusServiceUnavailable || status == http.StatusTooManyRequests) && len(body) == 0 && err == nil {
		return true
	}
	if len(body) > 0 && responseBodyIsOverload(body) {
		return true
	}
	if err != nil {
		return valueIsOverloadString(err.Error())
	}
	return false
}
func responseBodyIsOverload(body []byte) bool { return isOverloadErrorJSON(body) }
func isOverloadErrorJSON(body []byte) bool {
	for _, frame := range parseSSEFrames(body) {
		if frameIsOverload(frame) {
			return true
		}
	}
	var value any
	if json.Unmarshal(body, &value) == nil {
		return valueIsOverload(value)
	}
	return valueIsOverloadString(string(body))
}
func valueIsOverloadString(raw string) bool {
	trimmed := strings.TrimSpace(raw)
	var value any
	if json.Unmarshal([]byte(trimmed), &value) == nil && valueIsOverload(value) {
		return true
	}
	lower := strings.ToLower(trimmed)
	return strings.Contains(lower, "server_is_overloaded") || strings.Contains(lower, "server_overloaded") || strings.Contains(lower, "selected model is at capacity") || strings.Contains(lower, "our servers are currently overloaded") || (strings.Contains(lower, "capacity") && (strings.Contains(lower, "model") || strings.Contains(lower, "server")))
}
func waitBackoff(ctx context.Context, attempt int, cfg pluginConfig) error {
	if cfg.BackoffBaseMS <= 0 {
		return nil
	}
	delay := cfg.BackoffBaseMS * (1 << min(attempt, 5))
	if delay > cfg.BackoffMaxMS {
		delay = cfg.BackoffMaxMS
	}
	t := time.NewTimer(time.Duration(delay) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseSSEFrames(payload []byte) []sseFrame {
	decoder := &sseDecoder{}
	return decoder.feed(payload, true)
}
func decodeSSEFrame(raw []byte) (sseFrame, bool) {
	var data []byte
	eventType := ""
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		}
		if strings.HasPrefix(line, "data:") {
			if len(data) > 0 {
				data = append(data, '\n')
			}
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:"))...)
		}
	}
	var obj map[string]any
	if len(data) > 0 && json.Unmarshal(data, &obj) == nil {
		if typ, ok := obj["type"].(string); ok && typ != "" {
			eventType = typ
		}
	}
	if eventType == "" && len(data) == 0 {
		return sseFrame{}, false
	}
	return sseFrame{Raw: append([]byte(nil), raw...), JSON: obj, EventType: eventType}, true
}

func observeFrame(state *attemptState, frame sseFrame) {
	if frame.JSON == nil {
		return
	}
	if frameIsToolCall(frame) {
		state.ToolCall = true
		return
	}
	if frameIsGenerated(frame) {
		state.Generated = true
		state.SemanticOutput = true
	}
}
func frameIsOverload(frame sseFrame) bool { return valueIsOverload(frame.JSON) }
func valueIsOverload(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		if isCapacityCode(stringValue(v["code"])) {
			return true
		}
		if valueIsOverload(v["error"]) || valueIsOverload(v["response"]) {
			return true
		}
		for _, key := range []string{"message", "detail", "error_message"} {
			if valueIsOverloadString(stringValue(v[key])) {
				return true
			}
		}
	case []any:
		for _, item := range v {
			if valueIsOverload(item) {
				return true
			}
		}
	case string:
		return valueIsOverloadString(v)
	}
	return false
}
func isCapacityCode(code string) bool {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "server_is_overloaded", "server_overloaded", "slow_down", "service_unavailable_error":
		return true
	default:
		return false
	}
}
func findMessage(value any) string {
	switch v := value.(type) {
	case map[string]any:
		for _, key := range []string{"message", "detail", "error_message"} {
			if msg := stringValue(v[key]); msg != "" {
				return msg
			}
		}
		for _, child := range v {
			if msg := findMessage(child); msg != "" {
				return msg
			}
		}
	case []any:
		for _, child := range v {
			if msg := findMessage(child); msg != "" {
				return msg
			}
		}
	}
	return ""
}

func frameIsToolCall(frame sseFrame) bool {
	typ := strings.ToLower(frame.EventType)
	if strings.Contains(typ, "call") || strings.Contains(typ, "tool") || strings.Contains(typ, "computer") || strings.Contains(typ, "shell") || strings.Contains(typ, "mcp") || strings.Contains(typ, "apply_patch") {
		return true
	}
	if item, ok := frame.JSON["item"].(map[string]any); ok {
		return toolItemType(stringValue(item["type"]))
	}
	if response, ok := frame.JSON["response"].(map[string]any); ok {
		if item, ok := response["output_item"].(map[string]any); ok {
			return toolItemType(stringValue(item["type"]))
		}
	}
	return false
}
func toolItemType(typ string) bool {
	typ = strings.ToLower(strings.TrimSpace(typ))
	return strings.Contains(typ, "function_call") || strings.Contains(typ, "tool_call") || strings.Contains(typ, "computer_call") || strings.Contains(typ, "shell_call") || strings.Contains(typ, "custom_tool") || strings.Contains(typ, "tool_search") || strings.Contains(typ, "mcp_call") || strings.Contains(typ, "apply_patch")
}
func frameIsGenerated(frame sseFrame) bool {
	typ := strings.ToLower(frame.EventType)
	return strings.Contains(typ, "output_text") || strings.Contains(typ, "reasoning") || strings.Contains(typ, "content_part") || strings.Contains(typ, "image") || strings.Contains(typ, "audio") || containsSemanticKey(frame.JSON)
}
func containsSemanticKey(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			lower := strings.ToLower(key)
			if lower == "encrypted_content" || lower == "output_text" || lower == "text" || lower == "audio" || lower == "image" {
				if stringValue(child) != "" || child != nil {
					return true
				}
			}
			if containsSemanticKey(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if containsSemanticKey(child) {
				return true
			}
		}
	}
	return false
}
func rewriteCapacitySSEFrame(raw []byte) []byte {
	frame, ok := decodeSSEFrame(raw)
	if !ok || frame.JSON == nil {
		return raw
	}
	rewriteCapacityValue(frame.JSON, false)
	data, err := json.Marshal(frame.JSON)
	if err != nil {
		return raw
	}
	return replaceSSEData(raw, data)
}
func rewriteCapacityValue(value any, inError bool) {
	switch v := value.(type) {
	case map[string]any:
		if inError {
			if code, ok := v["code"].(string); ok && isCapacityCode(code) {
				v["code"] = "server_error"
			}
			if typ, ok := v["type"].(string); ok && (isCapacityCode(typ) || strings.EqualFold(typ, "error")) {
				v["type"] = "server_error"
			}
		}
		for key, child := range v {
			rewriteCapacityValue(child, inError || strings.EqualFold(key, "error"))
		}
	case []any:
		for _, child := range v {
			rewriteCapacityValue(child, inError)
		}
	}
}
func replaceSSEData(raw, data []byte) []byte {
	lines := strings.SplitAfter(string(raw), "\n")
	for i, line := range lines {
		trimmed := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if strings.HasPrefix(trimmed, "data:") {
			prefix := line[:strings.Index(line, "data:")+len("data:")]
			suffix := ""
			if strings.HasSuffix(line, "\r\n") {
				suffix = "\r\n"
			} else if strings.HasSuffix(line, "\n") {
				suffix = "\n"
			}
			lines[i] = prefix + " " + string(data) + suffix
			return []byte(strings.Join(lines, ""))
		}
	}
	return raw
}
func stringValue(v any) string { s, _ := v.(string); return s }
