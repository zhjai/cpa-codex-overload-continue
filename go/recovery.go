package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type sseFrame struct {
	Raw       []byte
	JSON      map[string]any
	EventType string
}

// sseDecoder keeps incomplete events across host stream reads. Host chunks are
// transport chunks, not SSE message boundaries, so parsing each chunk in
// isolation can otherwise hide an overload or split a delta.
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
	Generated  bool
	ToolCall   bool
	TextOutput bool
	Overloaded bool
	ResponseID string
	Partial    strings.Builder
}
type streamAttemptResult struct {
	State  attemptState
	Status int
	Err    error
}

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
		if err == nil && !responseBodyIsOverload(resp.Body) && resp.StatusCode < 500 {
			return okEnvelope(pluginapi.ExecutorResponse{Payload: resp.Body, Headers: resp.Headers})
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("upstream status %d", resp.StatusCode)
		}
		if !isOverloadError(resp.Body, resp.StatusCode, err) || attempt >= cfg.MaxPreCommitRetries {
			break
		}
		if errBackoff := waitBackoff(context.Background(), attempt, cfg); errBackoff != nil {
			lastErr = errBackoff
			break
		}
	}
	return errorEnvelope("server_is_overloaded", fmt.Sprintf("Codex upstream remained overloaded after bounded retry: %v", lastErr), http.StatusServiceUnavailable), nil
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
			closePluginStream(req.StreamID, err.Error())
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
		return fmt.Errorf("empty Responses request")
	}
	for attempt := 0; ; attempt++ {
		result := runStreamAttempt(req.ExecutorRequest, body, req.HostCallbackID, req.StreamID, cfg, false, "")
		if result.Err == nil {
			return nil
		}
		if !result.State.Overloaded {
			return result.Err
		}
		if !result.State.Generated && !result.State.ToolCall && attempt < cfg.MaxPreCommitRetries {
			if err := waitBackoff(context.Background(), attempt, cfg); err != nil {
				return err
			}
			continue
		}
		if result.State.TextOutput && !result.State.ToolCall && cfg.ContinuationMode == "text" && cfg.MaxContinuations > 0 {
			continuationBody, err := buildContinuationBody(body, result.State.Partial.String(), cfg.ContinuePrompt)
			if err != nil {
				return fmt.Errorf("safe continuation unavailable: %w", err)
			}
			cont := runStreamAttempt(req.ExecutorRequest, continuationBody, req.HostCallbackID, req.StreamID, cfg, true, result.State.ResponseID)
			if cont.Err == nil {
				return nil
			}
			return fmt.Errorf("continuation failed: %w", cont.Err)
		}
		return fmt.Errorf("server_is_overloaded: generated output or side effects prevent safe replay")
	}
}

func runStreamAttempt(req pluginapi.ExecutorRequest, body []byte, callbackID, pluginStreamID string, cfg pluginConfig, continuation bool, responseIDOverride string) streamAttemptResult {
	state := attemptState{}
	resp, err := hostModelExecuteStream(req, body, callbackID)
	if err != nil {
		state.Overloaded = isOverloadError(nil, hostErrorStatus(err), err)
		return streamAttemptResult{State: state, Status: hostErrorStatus(err), Err: err}
	}
	if resp.StatusCode >= 400 {
		state.Overloaded = isOverloadError(nil, resp.StatusCode, nil)
		return streamAttemptResult{State: state, Status: resp.StatusCode, Err: fmt.Errorf("upstream status %d", resp.StatusCode)}
	}
	if resp.StreamID == "" {
		return streamAttemptResult{State: state, Err: fmt.Errorf("host returned empty stream id")}
	}
	defer closeHostModelStream(resp.StreamID)
	var bootstrap [][]byte
	decoder := &sseDecoder{}
	for {
		chunk, errRead := readHostModelStream(resp.StreamID)
		if errRead != nil {
			state.Overloaded = isOverloadError(nil, hostErrorStatus(errRead), errRead)
			return streamAttemptResult{State: state, Status: hostErrorStatus(errRead), Err: errRead}
		}
		if chunk.Error != "" {
			state.Overloaded = isOverloadError(nil, 0, fmt.Errorf("%s", chunk.Error))
			return streamAttemptResult{State: state, Err: fmt.Errorf("%s", chunk.Error)}
		}
		if len(chunk.Payload) > 0 {
			frames := decoder.feed(chunk.Payload, false)
			for _, frame := range frames {
				observeFrame(&state, frame)
				if state.Overloaded {
					continue
				}
				if continuation && state.ToolCall {
					return streamAttemptResult{State: state, Err: fmt.Errorf("continuation produced a tool call; refusing side effects")}
				}
				payload := frame.Raw
				if continuation && cfg.RewriteResponseID && responseIDOverride != "" {
					payload = rewriteResponseID(frame.Raw, responseIDOverride)
				}
				if !state.Generated && !state.ToolCall && frame.EventType != "response.completed" {
					bootstrap = append(bootstrap, payload)
					continue
				}
				if state.Generated || state.ToolCall || frame.EventType == "response.completed" {
					for _, pending := range bootstrap {
						if err := emitPluginStreamChunk(pluginStreamID, pending); err != nil {
							return streamAttemptResult{State: state, Err: err}
						}
					}
					bootstrap = nil
				}
				if err := emitPluginStreamChunk(pluginStreamID, payload); err != nil {
					return streamAttemptResult{State: state, Err: err}
				}
			}
		}
		if chunk.Done {
			for _, frame := range decoder.feed(nil, true) {
				observeFrame(&state, frame)
				if state.Overloaded {
					continue
				}
				if continuation && state.ToolCall {
					return streamAttemptResult{State: state, Err: fmt.Errorf("continuation produced a tool call; refusing side effects")}
				}
				payload := frame.Raw
				if continuation && cfg.RewriteResponseID && responseIDOverride != "" {
					payload = rewriteResponseID(frame.Raw, responseIDOverride)
				}
				if !state.Generated && !state.ToolCall && frame.EventType != "response.completed" {
					bootstrap = append(bootstrap, payload)
					continue
				}
				for _, pending := range bootstrap {
					if err := emitPluginStreamChunk(pluginStreamID, pending); err != nil {
						return streamAttemptResult{State: state, Err: err}
					}
				}
				bootstrap = nil
				if err := emitPluginStreamChunk(pluginStreamID, payload); err != nil {
					return streamAttemptResult{State: state, Err: err}
				}
			}
			if state.Overloaded {
				return streamAttemptResult{State: state, Err: fmt.Errorf("server_is_overloaded")}
			}
			for _, pending := range bootstrap {
				if err := emitPluginStreamChunk(pluginStreamID, pending); err != nil {
					return streamAttemptResult{State: state, Err: err}
				}
			}
			return streamAttemptResult{State: state, Status: http.StatusOK}
		}
	}
}

func hostModelExecute(req pluginapi.ExecutorRequest, body []byte, callbackID string) (pluginapi.HostModelExecutionResponse, error) {
	raw, err := callHost(pluginabi.MethodHostModelExecute, hostModelExecutionRequest{HostModelExecutionRequest: pluginapi.HostModelExecutionRequest{EntryProtocol: "openai-response", ExitProtocol: "openai-response", Model: req.Model, Stream: false, Body: body, Headers: req.Headers, Query: req.Query, ForcedProvider: loadedConfig().Provider}, HostCallbackID: callbackID})
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
		s := strings.ToLower(err.Error())
		return strings.Contains(s, "server_is_overloaded") || strings.Contains(s, "server overloaded") || strings.Contains(s, "overloaded")
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
	return strings.Contains(strings.ToLower(string(body)), "server_is_overloaded")
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
	if frameIsOverload(frame) {
		state.Overloaded = true
		return
	}
	if state.ResponseID == "" {
		state.ResponseID = responseID(frame.JSON)
	}
	if frameIsToolCall(frame) {
		state.ToolCall = true
	}
	if frameIsGenerated(frame) {
		state.Generated = true
		if frameIsTextOutput(frame) {
			text := frameTextDelta(frame.JSON)
			if text == "" {
				return
			}
			state.TextOutput = true
			state.Partial.WriteString(text)
		}
	}
}
func frameIsOverload(frame sseFrame) bool { return valueIsOverload(frame.JSON) }
func valueIsOverload(value any) bool {
	m, ok := value.(map[string]any)
	if !ok {
		return false
	}
	if code := strings.ToLower(stringValue(m["code"])); code == "server_is_overloaded" || code == "server_overloaded" {
		return true
	}
	if e, ok := m["error"].(map[string]any); ok && valueIsOverload(e) {
		return true
	}
	if r, ok := m["response"].(map[string]any); ok && valueIsOverload(r) {
		return true
	}
	message := strings.ToLower(stringValue(m["message"]))
	return strings.Contains(message, "server") && strings.Contains(message, "overload")
}
func frameIsToolCall(frame sseFrame) bool {
	typ := strings.ToLower(frame.EventType)
	if strings.Contains(typ, "function_call") || strings.Contains(typ, "tool_call") || strings.Contains(typ, "computer_call") || strings.Contains(typ, "shell_call") || strings.Contains(typ, "custom_tool") || strings.Contains(typ, "tool_search") || strings.Contains(typ, "mcp_call") || strings.Contains(typ, "apply_patch") {
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
	return strings.Contains(typ, "output_text.delta") || strings.Contains(typ, "reasoning.delta") || strings.Contains(typ, "content_part.delta") || strings.Contains(typ, "output_text.done") || frameTextDelta(frame.JSON) != ""
}

func frameIsTextOutput(frame sseFrame) bool {
	typ := strings.ToLower(frame.EventType)
	return strings.Contains(typ, "output_text") || strings.Contains(typ, "content_part")
}
func frameTextDelta(m map[string]any) string {
	for _, key := range []string{"delta", "text", "output_text"} {
		if text := stringValue(m[key]); text != "" {
			return text
		}
	}
	return ""
}
func responseID(m map[string]any) string {
	if response, ok := m["response"].(map[string]any); ok {
		if id := stringValue(response["id"]); id != "" {
			return id
		}
	}
	return stringValue(m["response_id"])
}
func stringValue(v any) string { s, _ := v.(string); return s }

func rewriteResponseID(raw []byte, id string) []byte {
	frames := parseSSEFrames(raw)
	if len(frames) != 1 || frames[0].JSON == nil {
		return raw
	}
	obj := frames[0].JSON
	if response, ok := obj["response"].(map[string]any); ok {
		response["id"] = id
	}
	if _, ok := obj["response_id"]; ok {
		obj["response_id"] = id
	}
	data, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	old := extractFrameData(raw)
	if old == nil {
		return raw
	}
	return bytes.Replace(raw, old, append([]byte(" "), data...), 1)
}
func extractFrameData(raw []byte) []byte {
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "data:") {
			return []byte(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	return nil
}

func buildContinuationBody(original []byte, partial, prompt string) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(original, &payload); err != nil {
		return nil, fmt.Errorf("request is not a JSON object: %w", err)
	}
	if strings.TrimSpace(partial) == "" {
		return nil, fmt.Errorf("no generated text is available")
	}
	var items []json.RawMessage
	if raw, ok := payload["input"]; ok && len(raw) > 0 && raw[0] == '[' {
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, fmt.Errorf("decode input: %w", err)
		}
	} else if raw, ok := payload["input"]; ok {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return nil, fmt.Errorf("input must be a string or array")
		}
		items = append(items, mustJSON(map[string]any{"type": "message", "role": "user", "content": text}))
	}
	items = append(items, mustJSON(map[string]any{"type": "message", "role": "assistant", "content": partial}), mustJSON(map[string]any{"type": "message", "role": "user", "content": prompt}))
	payload["input"] = mustJSON(items)
	payload["stream"] = mustJSON(true)
	return json.Marshal(payload)
}
func mustJSON(v any) json.RawMessage { raw, _ := json.Marshal(v); return raw }
