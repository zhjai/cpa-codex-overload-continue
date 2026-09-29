package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

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
	if separator := hostChunkSeparator(d.buffer, payload); len(separator) > 0 {
		d.buffer = append(d.buffer, separator...)
	}
	d.buffer = append(d.buffer, payload...)
	if isCompleteCPADataChunk(payload) {
		d.buffer = append(d.buffer, '\n', '\n')
	}
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

func isCompleteCPADataChunk(payload []byte) bool {
	if !bytes.HasPrefix(payload, []byte("data:")) || bytes.ContainsAny(payload, "\r\n") {
		return false
	}
	return json.Valid(bytes.TrimSpace(payload[len("data:"):]))
}

func hostChunkSeparator(buffer, payload []byte) []byte {
	if len(buffer) == 0 || len(payload) == 0 || buffer[len(buffer)-1] == '\r' {
		return nil
	}
	field := sseFieldAtStart(payload)
	if field == "" {
		return nil
	}
	if buffer[len(buffer)-1] == '\n' {
		if field == "event" && hasSSEDataLine(buffer) {
			return []byte("\n")
		}
		return nil
	}
	if hasSSEDataLine(buffer) {
		return []byte("\n\n")
	}
	if field == "data" {
		return []byte("\n")
	}
	return []byte("\n\n")
}

func sseFieldAtStart(payload []byte) string {
	for _, field := range []string{"data:", "event:", "id:", "retry:"} {
		if bytes.HasPrefix(payload, []byte(field)) {
			return strings.TrimSuffix(field, ":")
		}
	}
	if bytes.HasPrefix(payload, []byte(":")) {
		return ":"
	}
	return ""
}

func hasSSEDataLine(buffer []byte) bool {
	for _, line := range strings.Split(strings.ReplaceAll(string(buffer), "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "data:") {
			return true
		}
	}
	return false
}

type attemptState struct {
	ToolCall       bool
	Forwarded      bool
	DataForwarded  bool
	SemanticOutput bool
	PlainText      bool
	NonTextOutput  bool
	Overloaded     bool
}

type streamAttemptResult struct {
	State         attemptState
	Err           error
	ClosePayload  string
	TerminalFrame []byte
}

type streamRuntime struct {
	execute func(pluginapi.ExecutorRequest, []byte, string) (pluginapi.HostModelStreamResponse, error)
	read    func(string) (pluginapi.HostModelStreamReadResponse, error)
	closeUp func(string)
	emit    func(string, []byte) error
	close   func(string, string)
}

func defaultStreamRuntime() streamRuntime {
	return streamRuntime{
		execute: hostModelExecuteStream,
		read:    readHostModelStream,
		closeUp: closeHostModelStream,
		emit:    emitPluginStreamChunk,
		close:   closePluginStream,
	}
}

// streamFailure carries an error payload that finishStream serializes as SSE.
type streamFailure struct {
	message string
	payload string
}

func (e *streamFailure) Error() string { return e.message }

func execute(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	body := requestBody(req.ExecutorRequest)
	if len(body) == 0 {
		return errorEnvelope("invalid_request", "empty Responses request", http.StatusBadRequest), nil
	}
	logRouteSelection(req.HostCallbackID, "executor", req.Model)
	resp, err := hostModelExecute(req.ExecutorRequest, body, req.HostCallbackID)
	if err != nil {
		return hostErrorEnvelope(err), nil
	}
	if resp.StatusCode >= 400 {
		code := "upstream_error"
		if isOverloadError(resp.Body, resp.StatusCode, nil) {
			code = "server_is_overloaded"
		}
		return errorEnvelope(code, string(resp.Body), resp.StatusCode), nil
	}
	return okEnvelope(pluginapi.ExecutorResponse{Payload: resp.Body, Headers: resp.Headers})
}

func executeStream(raw []byte) ([]byte, error) {
	return executeStreamWithRuntime(raw, defaultStreamRuntime())
}

func executeStreamWithRuntime(raw []byte, runtime streamRuntime) ([]byte, error) {
	var req rpcExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.StreamID) == "" {
		return errorEnvelope("invalid_request", "stream_id is required", http.StatusBadRequest), nil
	}
	// Establish the nested stream before returning the async handshake. Startup
	// failures can then preserve their native HTTP status and JSON error envelope.
	body := requestBody(req.ExecutorRequest)
	if len(body) == 0 {
		return errorEnvelope("invalid_request", "empty Responses request", http.StatusBadRequest), nil
	}
	logRouteSelection(req.HostCallbackID, "executor_stream", req.Model)
	resp, err := runtime.execute(req.ExecutorRequest, body, req.HostCallbackID)
	if err != nil {
		return hostErrorEnvelope(err), nil
	}
	if resp.StatusCode >= 400 {
		return errorEnvelope("upstream_error", fmt.Sprintf("upstream status %d", resp.StatusCode), resp.StatusCode), nil
	}
	if resp.StreamID == "" {
		return errorEnvelope("upstream_error", "host returned empty stream id", http.StatusBadGateway), nil
	}
	go func(hostStreamID string) {
		_ = finishStream(req, hostStreamID, runtime)
	}(resp.StreamID)
	return okEnvelope(map[string]any{"headers": map[string][]string{"Content-Type": {"text/event-stream"}}})
}

func hostErrorEnvelope(err error) []byte {
	status := hostErrorStatus(err)
	if status == 0 {
		status = http.StatusBadGateway
	}
	if payload := hostErrorPayload(err); payload != "" {
		var value map[string]any
		if json.Unmarshal([]byte(payload), &value) == nil {
			code := stringValue(value["code"])
			if code == "" {
				if nested, ok := value["error"].(map[string]any); ok {
					code = stringValue(nested["code"])
				}
			}
			if code == "" {
				code = "upstream_error"
			}
			return errorEnvelope(code, payload, status)
		}
	}
	return errorEnvelope("upstream_error", err.Error(), status)
}

func finishStream(req rpcExecutorRequest, hostStreamID string, runtime streamRuntime) error {
	err := runStreamWithRuntime(req, hostStreamID, runtime)
	if err != nil {
		failure, ok := err.(*streamFailure)
		if !ok {
			failure = &streamFailure{message: err.Error(), payload: originalFailurePayload(err.Error())}
		}
		if emitErr := runtime.emit(req.StreamID, streamFailureEvent(failure.payload)); emitErr != nil {
			runtime.close(req.StreamID, emitErr.Error())
			return emitErr
		}
	}
	runtime.close(req.StreamID, "")
	return nil
}

func runStreamWithRuntime(req rpcExecutorRequest, hostStreamID string, runtime streamRuntime) error {
	cfg := loadedConfig()
	body := requestBody(req.ExecutorRequest)
	if len(body) == 0 {
		return &streamFailure{message: "empty Responses request", payload: canonicalUpstreamError("empty Responses request")}
	}
	result := runStreamAttempt(req.StreamID, hostStreamID, cfg, runtime)
	if result.Err == nil {
		return nil
	}
	if len(result.TerminalFrame) > 0 {
		if err := runtime.emit(req.StreamID, result.TerminalFrame); err != nil {
			return err
		}
		return nil
	}
	payload := result.ClosePayload
	if payload == "" {
		if result.State.Overloaded {
			payload = capacityFailurePayload(result.Err.Error())
		} else {
			payload = originalFailurePayload(result.Err.Error())
		}
	}
	return &streamFailure{message: result.Err.Error(), payload: payload}
}

func runStreamAttempt(pluginStreamID, hostStreamID string, cfg pluginConfig, runtime streamRuntime) streamAttemptResult {
	state := attemptState{}
	if hostStreamID == "" {
		return streamAttemptResult{State: state, Err: fmt.Errorf("host returned empty stream id")}
	}
	defer runtime.closeUp(hostStreamID)
	decoder := &sseDecoder{}
	var pendingFrames [][]byte
	for {
		chunk, errRead := runtime.read(hostStreamID)
		if errRead != nil {
			raw := hostErrorPayload(errRead)
			if raw == "" {
				raw = errRead.Error()
			}
			result := streamErrorResult(state, raw, cfg)
			result.Err = errRead
			return result
		}
		if chunk.Error != "" {
			if err := emitPendingFrames(pluginStreamID, &pendingFrames, &state, runtime); err != nil {
				return streamAttemptResult{State: state, Err: err}
			}
			return streamErrorResult(state, chunk.Error, cfg)
		}
		if len(chunk.Payload) > 0 {
			for _, frame := range decoder.feed(chunk.Payload, false) {
				result, stop := forwardFrame(pluginStreamID, frame, &state, cfg, &pendingFrames, runtime)
				if result.Err != nil || stop {
					return result
				}
			}
		}
		if chunk.Done {
			for _, frame := range decoder.feed(nil, true) {
				result, stop := forwardFrame(pluginStreamID, frame, &state, cfg, &pendingFrames, runtime)
				if result.Err != nil || stop {
					return result
				}
			}
			if err := emitPendingFrames(pluginStreamID, &pendingFrames, &state, runtime); err != nil {
				return streamAttemptResult{State: state, Err: err}
			}
			if !state.DataForwarded {
				message := "upstream stream closed before first payload"
				return streamAttemptResult{State: state, Err: fmt.Errorf("%s", message), ClosePayload: canonicalUpstreamError(message)}
			}
			return streamAttemptResult{State: state}
		}
	}
}

func forwardFrame(pluginStreamID string, frame sseFrame, state *attemptState, cfg pluginConfig, pendingFrames *[][]byte, runtime streamRuntime) (streamAttemptResult, bool) {
	observeFrame(state, frame)
	if frameIsOverload(frame) {
		state.Overloaded = true
		terminalFrame := append([]byte(nil), frame.Raw...)
		if canRewriteCapacity(*state, cfg) {
			terminalFrame = rewriteCapacitySSEFrame(terminalFrame)
		}
		if err := emitPendingFrames(pluginStreamID, pendingFrames, state, runtime); err != nil {
			return streamAttemptResult{State: *state, Err: err}, false
		}
		return streamAttemptResult{State: *state, Err: fmt.Errorf("server_is_overloaded"), TerminalFrame: terminalFrame}, false
	}
	if !state.SemanticOutput && !state.ToolCall {
		*pendingFrames = append(*pendingFrames, append([]byte(nil), frame.Raw...))
		return streamAttemptResult{State: *state}, false
	}
	if err := emitPendingFrames(pluginStreamID, pendingFrames, state, runtime); err != nil {
		return streamAttemptResult{State: *state, Err: err}, false
	}
	if err := runtime.emit(pluginStreamID, frame.Raw); err != nil {
		return streamAttemptResult{State: *state, Err: err}, false
	}
	state.Forwarded = true
	if frameHasClientData(frame.Raw) {
		state.DataForwarded = true
	}
	return streamAttemptResult{State: *state}, false
}

func emitPendingFrames(pluginStreamID string, pendingFrames *[][]byte, state *attemptState, runtime streamRuntime) error {
	for _, frame := range *pendingFrames {
		if err := runtime.emit(pluginStreamID, frame); err != nil {
			return err
		}
		state.Forwarded = true
		if frameHasClientData(frame) {
			state.DataForwarded = true
		}
	}
	*pendingFrames = nil
	return nil
}

func streamErrorResult(state attemptState, raw string, cfg pluginConfig) streamAttemptResult {
	observeStructuredTerminal(&state, raw)
	state.Overloaded = isOverloadError(nil, 0, fmt.Errorf("%s", raw))
	payload := originalFailurePayload(raw)
	if state.Overloaded {
		if canRewriteCapacity(state, cfg) {
			if json.Valid([]byte(strings.TrimSpace(raw))) {
				payload = rewriteCapacityJSONPayload(raw)
			} else {
				payload = normalizedServerErrorPayload(raw)
			}
		} else {
			payload = capacityFailurePayload(raw)
		}
	}
	return streamAttemptResult{State: state, Err: fmt.Errorf("%s", raw), ClosePayload: payload}
}

func observeStructuredTerminal(state *attemptState, raw string) {
	var value map[string]any
	if json.Unmarshal([]byte(strings.TrimSpace(raw)), &value) != nil {
		return
	}
	observeFrame(state, sseFrame{JSON: value, EventType: stringValue(value["type"])})
}

func rewriteCapacityJSONPayload(raw string) string {
	var value any
	if json.Unmarshal([]byte(strings.TrimSpace(raw)), &value) != nil {
		return normalizedServerErrorPayload(raw)
	}
	rewriteCapacityValue(value, false)
	data, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	return string(data)
}

func streamCommitted(state attemptState) bool {
	return state.Forwarded || state.SemanticOutput || state.ToolCall
}

func canRewriteCapacity(state attemptState, cfg pluginConfig) bool {
	return cfg.PostOutputCapacity == "text_only" && state.SemanticOutput && state.PlainText && !state.NonTextOutput && !state.ToolCall
}

func capacityFailurePayload(raw string) string {
	trimmed := strings.TrimSpace(raw)
	var value any
	if json.Unmarshal([]byte(trimmed), &value) == nil && valueIsOverload(value) {
		return trimmed
	}
	code := "server_is_overloaded"
	lower := strings.ToLower(trimmed)
	for _, candidate := range []string{"server_is_overloaded", "server_overloaded", "slow_down", "service_unavailable_error"} {
		if strings.Contains(lower, candidate) {
			code = candidate
			break
		}
	}
	message := errorMessage(trimmed)
	if prefix, rest, found := strings.Cut(message, ":"); found && isCapacityCode(prefix) {
		message = strings.TrimSpace(rest)
	}
	return marshalErrorPayload("server_error", code, message)
}

func streamFailureEvent(payload string) []byte {
	trimmed := strings.TrimSpace(payload)
	if frames := parseSSEFrames([]byte(trimmed)); len(frames) > 0 {
		return frames[0].Raw
	}
	var value any
	if json.Unmarshal([]byte(trimmed), &value) != nil {
		trimmed = originalFailurePayload(trimmed)
		_ = json.Unmarshal([]byte(trimmed), &value)
	}
	data, err := json.Marshal(value)
	if err != nil {
		data = []byte(canonicalUpstreamError("upstream stream failed"))
	}
	event := "error"
	if obj, ok := value.(map[string]any); ok && stringValue(obj["type"]) == "response.failed" {
		event = "response.failed"
	}
	return []byte("event: " + event + "\ndata: " + string(data) + "\n\n")
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
	raw, err := callHost(pluginabi.MethodHostModelExecute, hostModelExecutionRequest{HostModelExecutionRequest: pluginapi.HostModelExecutionRequest{EntryProtocol: "openai-response", ExitProtocol: "openai-response", Model: req.Model, Stream: false, Body: body, Headers: req.Headers, Query: req.Query, Alt: req.Alt, ForcedProvider: loadedConfig().Provider}, HostCallbackID: callbackID})
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
	if len(body) > 0 && responseBodyIsOverload(body) {
		return true
	}
	if err != nil {
		if payload := hostErrorPayload(err); payload != "" {
			return responseBodyIsOverload([]byte(payload))
		}
		return valueIsOverloadString(err.Error())
	}
	_ = status
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
	return strings.Contains(lower, "server_is_overloaded") || strings.Contains(lower, "server_overloaded") || strings.Contains(lower, "selected model is at capacity") || strings.Contains(lower, "our servers are currently overloaded")
}
func parseSSEFrames(payload []byte) []sseFrame {
	decoder := &sseDecoder{}
	return decoder.feed(payload, true)
}

func frameHasClientData(raw []byte) bool {
	frame, ok := decodeSSEFrame(raw)
	if ok && frame.JSON != nil {
		return true
	}
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if strings.TrimSpace(strings.TrimPrefix(line, "data:")) == "[DONE]" && strings.HasPrefix(line, "data:") {
			return true
		}
	}
	return false
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
	if frameIsToolCall(frame) {
		state.ToolCall = true
		return
	}
	semantic, plainText := frameOutputKind(frame)
	if semantic {
		state.SemanticOutput = true
		if plainText {
			state.PlainText = true
		} else {
			state.NonTextOutput = true
		}
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
	return containsToolItem(frame.JSON)
}

func containsToolItem(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		if toolItemType(stringValue(v["type"])) {
			return true
		}
		for _, child := range v {
			if containsToolItem(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if containsToolItem(child) {
				return true
			}
		}
	}
	return false
}
func toolItemType(typ string) bool {
	typ = strings.ToLower(strings.TrimSpace(typ))
	return strings.Contains(typ, "function_call") || strings.Contains(typ, "tool_call") || strings.Contains(typ, "computer_call") || strings.Contains(typ, "shell_call") || strings.Contains(typ, "custom_tool") || strings.Contains(typ, "tool_search") || strings.Contains(typ, "mcp_call") || strings.Contains(typ, "apply_patch")
}
func frameOutputKind(frame sseFrame) (semantic, plainText bool) {
	typ := strings.ToLower(frame.EventType)
	if typ == "response.created" || typ == "response.in_progress" {
		return false, false
	}
	if isNonTextEventType(typ) || hasNonTextOutput(frame.JSON) {
		return true, false
	}
	if strings.Contains(typ, "output_text") {
		plainText = nonEmptyString(frame.JSON["delta"]) || nonEmptyString(frame.JSON["text"])
		return plainText, plainText
	}
	if hasTypedOutputText(frame.JSON) {
		return true, true
	}
	return false, false
}

func hasTypedOutputText(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		typ := strings.ToLower(stringValue(v["type"]))
		if (typ == "output_text" || typ == "text") && nonEmptyString(v["text"]) {
			return true
		}
		for key, child := range v {
			if strings.EqualFold(key, "output_text") && nonEmptyString(child) {
				return true
			}
			if hasTypedOutputText(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if hasTypedOutputText(child) {
				return true
			}
		}
	}
	return false
}

func hasNonTextOutput(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		typ := strings.ToLower(stringValue(v["type"]))
		if isNonTextEventType(typ) {
			return true
		}
		for key, child := range v {
			lower := strings.ToLower(key)
			if lower == "encrypted_content" && nonEmptyString(child) || (lower == "image" || lower == "audio") && child != nil {
				return true
			}
			if hasNonTextOutput(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if hasNonTextOutput(child) {
				return true
			}
		}
	}
	return false
}

func isNonTextEventType(typ string) bool {
	for _, marker := range []string{"reasoning", "encrypted_content", "image", "audio", "refusal", "transcription", "transcript", "video", "spatial", "code_interpreter", "computer_call", "function_call", "custom_tool", "mcp_call", "shell_call"} {
		if strings.Contains(typ, marker) {
			return true
		}
	}
	return false
}

func nonEmptyString(value any) bool { return strings.TrimSpace(stringValue(value)) != "" }
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
		if inError && errorMapIsCapacity(v) {
			if code, ok := v["code"].(string); ok && isCapacityCode(code) {
				v["code"] = "server_error"
			} else if _, exists := v["code"]; !exists {
				v["code"] = "server_error"
			}
			if typ, ok := v["type"].(string); ok && (isCapacityCode(typ) || strings.EqualFold(typ, "error")) {
				v["type"] = "server_error"
			} else if _, exists := v["type"]; !exists {
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

func errorMapIsCapacity(value map[string]any) bool {
	if isCapacityCode(stringValue(value["code"])) || isCapacityCode(stringValue(value["type"])) {
		return true
	}
	for _, key := range []string{"message", "detail", "error_message"} {
		if valueIsOverloadString(stringValue(value[key])) {
			return true
		}
	}
	return false
}

func replaceSSEData(raw, data []byte) []byte {
	lines := strings.SplitAfter(string(raw), "\n")
	var out strings.Builder
	replaced := false
	for _, line := range lines {
		trimmed := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if strings.HasPrefix(trimmed, "data:") {
			if replaced {
				continue
			}
			suffix := ""
			if strings.HasSuffix(line, "\r\n") {
				suffix = "\r\n"
			} else if strings.HasSuffix(line, "\n") {
				suffix = "\n"
			}
			out.WriteString("data: ")
			out.Write(data)
			out.WriteString(suffix)
			replaced = true
			continue
		}
		out.WriteString(line)
	}
	if replaced {
		return []byte(out.String())
	}
	return raw
}
func stringValue(v any) string { s, _ := v.(string); return s }
