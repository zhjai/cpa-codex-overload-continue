package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestParseAndClassifyNestedCapacitySSE(t *testing.T) {
	payload := []byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity\"}}}\n\n")
	frames := parseSSEFrames(payload)
	if len(frames) != 1 || !frameIsOverload(frames[0]) {
		t.Fatalf("frames = %#v", frames)
	}
}

func TestSSEDecoderCarriesFramesAcrossChunks(t *testing.T) {
	decoder := &sseDecoder{}
	part := []byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"slow_down\"}}\n")
	if got := decoder.feed(part, false); len(got) != 0 {
		t.Fatalf("incomplete frame was emitted: %#v", got)
	}
	got := decoder.feed([]byte("\n"), false)
	if len(got) != 1 || !frameIsOverload(got[0]) {
		t.Fatalf("decoded frames = %#v", got)
	}
}

func TestSSEDecoderSeparatesEventAfterSingleNewline(t *testing.T) {
	decoder := &sseDecoder{}
	first := []byte("event: response.created\ndata: {\"type\":\"response.created\"}\n")
	if got := decoder.feed(first, false); len(got) != 0 {
		t.Fatalf("incomplete frame was emitted: %#v", got)
	}
	second := []byte("event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")
	got := decoder.feed(second, false)
	if len(got) != 2 || got[0].EventType != "response.created" || got[1].EventType != "response.completed" {
		t.Fatalf("adjacent frames were merged: %#v", got)
	}
}

func TestSSEDecoderReconstructsCPAEventAndDataChunks(t *testing.T) {
	decoder := &sseDecoder{}
	if got := decoder.feed([]byte("event: response.failed"), false); len(got) != 0 {
		t.Fatalf("event-only chunk emitted a frame: %#v", got)
	}
	got := decoder.feed([]byte(`data: {"type":"response.failed","response":{"error":{"code":"server_is_overloaded"}}}`), false)
	if len(got) != 1 || !frameIsOverload(got[0]) || !strings.Contains(string(got[0].Raw), "\n\n") {
		t.Fatalf("decoded frame = %#v", got)
	}
}

func TestSSEDecoderSeparatesDataOnlyCPAChunks(t *testing.T) {
	decoder := &sseDecoder{}
	first := []byte(`data: {"type":"response.created"}`)
	second := []byte(`data: {"type":"response.completed"}`)
	got := decoder.feed(first, false)
	if len(got) != 1 || got[0].EventType != "response.created" {
		t.Fatalf("first decoded frame = %#v", got)
	}
	got = decoder.feed(second, false)
	if len(got) != 1 || got[0].EventType != "response.completed" {
		t.Fatalf("decoded second frame = %#v", got)
	}
	if got := decoder.feed(nil, true); len(got) != 0 {
		t.Fatalf("unexpected terminal frames = %#v", got)
	}
}

func TestCapacityMessageWithoutCodeIsRecognized(t *testing.T) {
	for _, msg := range []string{"Selected model is at capacity. Please try a different model.", "Our servers are currently overloaded; try again later."} {
		if !isOverloadErrorJSON([]byte(`{"error":{"message":"` + msg + `"}}`)) {
			t.Fatalf("message not recognized: %q", msg)
		}
	}
}

func TestNonCapacityErrorIsNotOverload(t *testing.T) {
	if isOverloadErrorJSON([]byte(`{"error":{"code":"invalid_request_error","message":"bad input"}}`)) {
		t.Fatal("invalid request was classified as overload")
	}
	for _, message := range []string{"model capacity setting is invalid", "server capacity metrics unavailable"} {
		if valueIsOverloadString(message) {
			t.Fatalf("unrelated message classified as capacity: %s", message)
		}
	}
}

func TestHTTPStatusAloneDoesNotClassifyCapacity(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		if isOverloadError(nil, status, nil) {
			t.Fatalf("status %d alone was classified as capacity", status)
		}
	}
	for _, payload := range []string{
		`{"error":{"code":"usage_limit_reached","message":"usage limit reached"}}`,
		`{"error":{"code":"model_cooldown","message":"all credentials are cooling down"}}`,
	} {
		if isOverloadError([]byte(payload), http.StatusTooManyRequests, nil) {
			t.Fatalf("non-capacity payload classified as capacity: %s", payload)
		}
	}
}

func TestHostErrorJSONIsUsedForCapacityClassification(t *testing.T) {
	err := &hostRPCError{Code: "host_call_failed", Message: `{"error":{"code":"server_is_overloaded","message":"Selected model is at capacity"}}`, HTTPStatus: http.StatusServiceUnavailable}
	if !isOverloadError(nil, err.HTTPStatus, err) {
		t.Fatal("structured host capacity error was not recognized")
	}
}

func TestHostErrorEnvelopePreservesStatusAndCode(t *testing.T) {
	err := &hostRPCError{Code: "host_call_failed", Message: `{"error":{"code":"usage_limit_reached","message":"quota reached"}}`, HTTPStatus: http.StatusTooManyRequests}
	var env envelope
	if err := json.Unmarshal(hostErrorEnvelope(err), &env); err != nil {
		t.Fatal(err)
	}
	if env.Error == nil || env.Error.HTTPStatus != http.StatusTooManyRequests || env.Error.Code != "usage_limit_reached" {
		t.Fatalf("error envelope = %#v", env.Error)
	}
}

func TestExecuteStreamReturnsSynchronousStartupError(t *testing.T) {
	host := &fakeStreamHost{executeErrors: []error{&hostRPCError{Code: "host_call_failed", Message: `{"error":{"code":"usage_limit_reached","message":"quota reached"}}`, HTTPStatus: http.StatusTooManyRequests}}}
	request, err := json.Marshal(rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)},
		StreamID:        "outer",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := executeStreamWithRuntime(request, host.runtime())
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if json.Unmarshal(raw, &env) != nil || env.OK || env.Error == nil {
		t.Fatalf("startup error envelope = %s", raw)
	}
	if env.Error.HTTPStatus != http.StatusTooManyRequests || env.Error.Code != "usage_limit_reached" {
		t.Fatalf("startup error = %#v", env.Error)
	}
	if !strings.Contains(env.Error.Message, `"usage_limit_reached"`) {
		t.Fatalf("startup JSON body was lost: %q", env.Error.Message)
	}
	if host.executeCalls != 1 || len(host.readIDs) != 0 || len(host.emitIDs) != 0 || len(host.closeIDs) != 0 {
		t.Fatalf("startup callbacks: execute=%d read=%#v emit=%#v close=%#v", host.executeCalls, host.readIDs, host.emitIDs, host.closeIDs)
	}
}

func TestSemanticOutputAndToolCallState(t *testing.T) {
	state := attemptState{}
	for _, raw := range []string{
		"event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
		"event: response.reasoning.delta\ndata: {\"type\":\"response.reasoning.delta\",\"delta\":\"private\"}\n\n",
	} {
		for _, frame := range parseSSEFrames([]byte(raw)) {
			observeFrame(&state, frame)
		}
	}
	if !state.SemanticOutput || state.PlainText || state.ToolCall {
		t.Fatalf("reasoning state = %#v", state)
	}
	tool := sseFrame{EventType: "response.output_item.added", JSON: map[string]any{"item": map[string]any{"type": "computer_call"}}}
	observeFrame(&state, tool)
	if !state.ToolCall {
		t.Fatal("tool call was not detected")
	}
	if canRewriteCapacity(state, pluginConfig{PostOutputCapacity: "text_only"}) {
		t.Fatal("reasoning plus tool output was eligible for rewrite")
	}
}

func TestRewriteCapacitySSEFrameOnlyChangesErrorCode(t *testing.T) {
	raw := []byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"r1\",\"sequence_number\":7,\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity\"}}}\n\n")
	got := string(rewriteCapacitySSEFrame(raw))
	if !strings.Contains(got, `"code":"server_error"`) || !strings.Contains(got, `"id":"r1"`) || !strings.Contains(got, `"sequence_number":7`) || !strings.Contains(got, "event: response.failed") {
		t.Fatalf("rewritten = %s", got)
	}
}

func TestRewriteRequiresTextOnlySemanticOutputAndNoTools(t *testing.T) {
	if canRewriteCapacity(attemptState{PlainText: true}, pluginConfig{PostOutputCapacity: "fail_closed"}) {
		t.Fatal("fail_closed rewrote capacity")
	}
	if canRewriteCapacity(attemptState{PlainText: true, ToolCall: true}, pluginConfig{PostOutputCapacity: "text_only"}) {
		t.Fatal("tool turn rewrote capacity")
	}
	if canRewriteCapacity(attemptState{SemanticOutput: true}, pluginConfig{PostOutputCapacity: "text_only"}) {
		t.Fatal("reasoning-only output rewrote capacity")
	}
	if canRewriteCapacity(attemptState{PlainText: true, NonTextOutput: true}, pluginConfig{PostOutputCapacity: "text_only"}) {
		t.Fatal("mixed text and non-text output rewrote capacity")
	}
	if !canRewriteCapacity(attemptState{SemanticOutput: true, PlainText: true}, pluginConfig{PostOutputCapacity: "text_only"}) {
		t.Fatal("text-only output was not eligible")
	}
	var refusal attemptState
	observeFrame(&refusal, sseFrame{EventType: "response.refusal.delta", JSON: map[string]any{"type": "response.refusal.delta", "delta": "I cannot help"}})
	if !streamCommitted(refusal) || canRewriteCapacity(refusal, pluginConfig{PostOutputCapacity: "text_only"}) {
		t.Fatalf("refusal state was not handled conservatively: %#v", refusal)
	}
}

func TestPreOutputCapacityFailurePreservesCapacityCode(t *testing.T) {
	state := attemptState{Overloaded: true}
	if streamCommitted(state) {
		t.Fatal("empty pre-output attempt was marked committed")
	}
	got := streamErrorResult(state, `{"error":{"code":"server_is_overloaded"}}`, pluginConfig{PostOutputCapacity: "text_only"})
	if !strings.Contains(got.ClosePayload, `"code":"server_is_overloaded"`) {
		t.Fatalf("pre-output close payload = %q", got.ClosePayload)
	}
}

func TestStructuredClosePayloadPreservesNonJSONError(t *testing.T) {
	got := originalFailurePayload("server_is_overloaded")
	if !strings.Contains(got, `"code":"upstream_error"`) || !strings.Contains(got, "server_is_overloaded") {
		t.Fatalf("payload = %s", got)
	}
	got = normalizedServerErrorPayload(`{"error":{"code":"slow_down","message":"Selected model is at capacity"}}`)
	if !strings.Contains(got, `"code":"server_error"`) || !strings.Contains(got, "Selected model is at capacity") {
		t.Fatalf("normalized = %s", got)
	}
}

func TestHostModelRequestPreservesAlternateRoute(t *testing.T) {
	request := pluginapi.ExecutorRequest{Model: "gpt-test", Alt: "responses/compact", AuthID: "outer-auth"}
	got := hostModelRequest(request, []byte(`{"model":"gpt-test"}`), "callback")
	if got.Alt != "responses/compact" {
		t.Fatalf("host Alt = %q", got.Alt)
	}
	if got.AuthID != "" {
		t.Fatalf("nested host request pinned outer auth %q", got.AuthID)
	}
}

type fakeStreamHost struct {
	streams       [][]pluginapi.HostModelStreamReadResponse
	executeErrors []error
	indexes       map[string]int
	streamIndex   int
	executeCalls  int
	emitted       [][]byte
	closedWith    []string
	emitIDs       []string
	closeIDs      []string
	readIDs       []string
	closeUpIDs    []string
	done          chan struct{}
}

func (h *fakeStreamHost) runtime() streamRuntime {
	h.indexes = make(map[string]int)
	return streamRuntime{
		execute: func(pluginapi.ExecutorRequest, []byte, string) (pluginapi.HostModelStreamResponse, error) {
			h.executeCalls++
			index := h.streamIndex
			h.streamIndex++
			if index < len(h.executeErrors) && h.executeErrors[index] != nil {
				return pluginapi.HostModelStreamResponse{}, h.executeErrors[index]
			}
			id := fmt.Sprintf("stream-%d", index)
			h.indexes[id] = 0
			return pluginapi.HostModelStreamResponse{StatusCode: http.StatusOK, StreamID: id}, nil
		},
		read: func(id string) (pluginapi.HostModelStreamReadResponse, error) {
			h.readIDs = append(h.readIDs, id)
			index := h.streamIndexFromID(id)
			if index >= len(h.streams) || h.indexes[id] >= len(h.streams[index]) {
				return pluginapi.HostModelStreamReadResponse{Done: true}, nil
			}
			chunk := h.streams[index][h.indexes[id]]
			h.indexes[id]++
			return chunk, nil
		},
		closeUp: func(id string) { h.closeUpIDs = append(h.closeUpIDs, id) },
		emit: func(id string, payload []byte) error {
			h.emitIDs = append(h.emitIDs, id)
			h.emitted = append(h.emitted, append([]byte(nil), payload...))
			return nil
		},
		close: func(id string, message string) {
			h.closeIDs = append(h.closeIDs, id)
			h.closedWith = append(h.closedWith, message)
			if h.done != nil {
				close(h.done)
			}
		},
	}
}

func (h *fakeStreamHost) streamIndexFromID(id string) int {
	var index int
	_, _ = fmt.Sscanf(id, "stream-%d", &index)
	return index
}

func lineChunks(lines ...string) []pluginapi.HostModelStreamReadResponse {
	chunks := make([]pluginapi.HostModelStreamReadResponse, 0, len(lines)+1)
	for _, line := range lines {
		chunks = append(chunks, pluginapi.HostModelStreamReadResponse{Payload: []byte(line)})
	}
	return append(chunks, pluginapi.HostModelStreamReadResponse{Done: true})
}

func TestExecuteStreamUsesOneNestedStreamAndKeepsIDsSeparate(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"gpt-test"}, PostOutputCapacity: "fail_closed"})
	host := &fakeStreamHost{
		streams: [][]pluginapi.HostModelStreamReadResponse{lineChunks(
			"event: response.completed",
			`data: {"type":"response.completed","response":{"id":"resp-1","status":"completed"}}`,
		)},
		done: make(chan struct{}),
	}
	request, err := json.Marshal(rpcExecutorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test","stream":true}`)},
		StreamID:        "outer",
		HostCallbackID:  "callback-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := executeStreamWithRuntime(request, host.runtime())
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || !env.OK {
		t.Fatalf("stream handshake = %s, err = %v", raw, err)
	}
	select {
	case <-host.done:
	case <-time.After(2 * time.Second):
		t.Fatal("async plugin stream did not close")
	}
	if host.executeCalls != 1 {
		t.Fatalf("execute calls = %d, want 1", host.executeCalls)
	}
	if len(host.readIDs) == 0 || len(host.closeUpIDs) != 1 || host.closeUpIDs[0] != "stream-0" {
		t.Fatalf("nested stream ids: read=%#v close=%#v", host.readIDs, host.closeUpIDs)
	}
	for _, id := range host.readIDs {
		if id != "stream-0" {
			t.Fatalf("nested read id = %q, want stream-0", id)
		}
	}
	for _, id := range host.emitIDs {
		if id != "outer" {
			t.Fatalf("plugin emit id = %q, want outer", id)
		}
	}
	if len(host.closeIDs) != 1 || host.closeIDs[0] != "outer" {
		t.Fatalf("plugin close ids = %#v, want outer", host.closeIDs)
	}
}

func TestFinishStreamDoesNotReplayPreOutputCapacityFailure(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"gpt-test"}, PostOutputCapacity: "fail_closed", MaxPreOutputRetries: 0})
	host := &fakeStreamHost{streams: [][]pluginapi.HostModelStreamReadResponse{
		lineChunks(
			"event: response.created",
			`data: {"type":"response.created","response":{"id":"failed"}}`,
			"event: response.in_progress",
			`data: {"type":"response.in_progress"}`,
			"event: response.failed",
			`data: {"type":"response.failed","response":{"error":{"code":"server_is_overloaded","message":"Selected model is at capacity"}}}`,
		),
	}}
	host.indexes = map[string]int{"stream-0": 0}
	if err := finishStream(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)}, StreamID: "outer"}, "stream-0", host.runtime()); err != nil {
		t.Fatal(err)
	}
	joined := string(bytes.Join(host.emitted, nil))
	if !strings.Contains(joined, "server_is_overloaded") {
		t.Fatalf("emitted stream = %s", joined)
	}
	if len(host.emitted) < 3 || !strings.Contains(string(host.emitted[0]), "response.created") || !strings.Contains(string(host.emitted[1]), "response.in_progress") || !strings.Contains(string(host.emitted[len(host.emitted)-1]), "response.failed") {
		t.Fatalf("pending frames were not emitted before terminal frame: %#v", host.emitted)
	}
	if len(host.closedWith) != 1 || host.closedWith[0] != "" {
		t.Fatalf("close messages = %#v, want clean close", host.closedWith)
	}
	for _, id := range host.emitIDs {
		if id != "outer" {
			t.Fatalf("emit id = %q, want plugin stream outer", id)
		}
	}
	if len(host.closeIDs) != 1 || host.closeIDs[0] != "outer" || len(host.closeUpIDs) != 1 || host.closeUpIDs[0] != "stream-0" {
		t.Fatalf("stream ids: emit=%#v close=%#v closeUp=%#v", host.emitIDs, host.closeIDs, host.closeUpIDs)
	}
}

func TestFinishStreamEmitsHostOverloadAsSSEAndClosesCleanly(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", PostOutputCapacity: "fail_closed"})
	host := &fakeStreamHost{executeErrors: []error{&hostRPCError{Code: "server_is_overloaded", Message: "Selected model is at capacity", HTTPStatus: http.StatusServiceUnavailable}}}
	host.streams = [][]pluginapi.HostModelStreamReadResponse{{{Error: "server_is_overloaded: capacity"}}}
	host.indexes = map[string]int{"stream-0": 0}
	err := finishStream(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)}, StreamID: "outer"}, "stream-0", host.runtime())
	if err != nil {
		t.Fatal(err)
	}
	if len(host.emitted) != 1 || !strings.HasPrefix(string(host.emitted[0]), "event: error\ndata: ") {
		t.Fatalf("terminal output = %#v", host.emitted)
	}
	frames := parseSSEFrames(host.emitted[0])
	if len(frames) != 1 || !frameIsOverload(frames[0]) {
		t.Fatalf("terminal error frame = %#v", frames)
	}
	var payload map[string]any
	parts := strings.SplitN(string(host.emitted[0]), "data: ", 2)
	if len(parts) != 2 {
		t.Fatalf("terminal SSE missing data field: %q", host.emitted[0])
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(parts[1])), &payload); err != nil {
		t.Fatalf("decode terminal payload: %v", err)
	}
	if !strings.Contains(string(host.emitted[0]), "server_is_overloaded") {
		t.Fatalf("terminal payload lost overload code: %s", host.emitted[0])
	}
	if len(host.closedWith) != 1 || host.closedWith[0] != "" {
		t.Fatalf("close messages = %#v, want clean close", host.closedWith)
	}
}

func TestFinishStreamNeverReplaysAfterTextOrToolOutput(t *testing.T) {
	tests := []struct {
		name   string
		chunks []pluginapi.HostModelStreamReadResponse
	}{
		{
			name: "text",
			chunks: []pluginapi.HostModelStreamReadResponse{
				{Payload: []byte("event: response.output_text.delta")},
				{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"partial"}`)},
				{Error: "server_is_overloaded: capacity"},
			},
		},
		{
			name: "tool call",
			chunks: lineChunks(
				"event: response.output_item.added",
				`data: {"type":"response.output_item.added","item":{"type":"function_call","call_id":"call-1"}}`,
				"event: response.failed",
				`data: {"type":"response.failed","response":{"error":{"code":"server_is_overloaded"}}}`,
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"gpt-test"}, PostOutputCapacity: "fail_closed", MaxPreOutputRetries: 0})
			host := &fakeStreamHost{streams: [][]pluginapi.HostModelStreamReadResponse{test.chunks}, indexes: map[string]int{"stream-0": 0}}
			err := finishStream(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)}, StreamID: "outer"}, "stream-0", host.runtime())
			if err != nil {
				t.Fatal(err)
			}
			if len(host.emitted) < 2 {
				t.Fatalf("emitted chunks = %#v", host.emitted)
			}
			if len(host.closedWith) != 1 || host.closedWith[0] != "" {
				t.Fatalf("close messages = %#v, want clean close", host.closedWith)
			}
		})
	}
}

func TestFinishStreamDetectsOutputEmbeddedInCapacityFailure(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", PostOutputCapacity: "fail_closed"})
	host := &fakeStreamHost{streams: [][]pluginapi.HostModelStreamReadResponse{lineChunks(
		"event: response.failed",
		`data: {"type":"response.failed","response":{"status":"failed","output":[{"type":"message","content":[{"type":"output_text","text":"partial output"}]},{"type":"function_call","call_id":"call-1"}],"error":{"code":"server_is_overloaded"}}}`,
	)}, indexes: map[string]int{"stream-0": 0}}
	err := finishStream(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)}, StreamID: "outer"}, "stream-0", host.runtime())
	if err != nil {
		t.Fatal(err)
	}
	if len(host.emitted) != 1 || !strings.Contains(string(host.emitted[0]), "server_is_overloaded") {
		t.Fatalf("terminal output = %#v", host.emitted)
	}
}

func TestFinishStreamTextOnlyRejectsMixedTerminalOutput(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"gpt-test"}, PostOutputCapacity: "text_only"})
	terminal := `{"type":"response.failed","response":{"output":[{"type":"message","content":[{"type":"output_text","text":"partial"}]},{"type":"reasoning","encrypted_content":"opaque"},{"type":"function_call","call_id":"call-1"}],"error":{"code":"server_is_overloaded","message":"capacity"}}}`
	host := &fakeStreamHost{streams: [][]pluginapi.HostModelStreamReadResponse{{
		{Payload: []byte("event: response.output_text.delta")},
		{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"partial"}`)},
		{Error: terminal, Done: true},
	}}}
	if err := finishStream(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)}, StreamID: "outer"}, "stream-0", host.runtime()); err != nil {
		t.Fatal(err)
	}
	joined := string(bytes.Join(host.emitted, nil))
	if !strings.Contains(joined, `"code":"server_is_overloaded"`) || strings.Contains(joined, `"code":"server_error"`) {
		t.Fatalf("mixed terminal output was rewritten: %s", joined)
	}
}

func TestFinishStreamHandlesPlainCapacityErrorsByMode(t *testing.T) {
	for _, test := range []struct {
		mode     string
		wantCode string
	}{
		{mode: "fail_closed", wantCode: "server_is_overloaded"},
		{mode: "text_only", wantCode: "server_error"},
	} {
		t.Run(test.mode, func(t *testing.T) {
			currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"gpt-test"}, PostOutputCapacity: test.mode})
			host := &fakeStreamHost{streams: [][]pluginapi.HostModelStreamReadResponse{{
				{Payload: []byte("event: response.output_text.delta")},
				{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"partial"}`)},
				{Error: "server_is_overloaded: Selected model is at capacity", Done: true},
			}}}
			if err := finishStream(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)}, StreamID: "outer"}, "stream-0", host.runtime()); err != nil {
				t.Fatal(err)
			}
			last := host.emitted[len(host.emitted)-1]
			if !strings.Contains(string(last), `"code":"`+test.wantCode+`"`) {
				t.Fatalf("terminal error = %s", last)
			}
		})
	}
}

func TestFinishStreamPreservesRPCReadErrorMessage(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"gpt-test"}, PostOutputCapacity: "fail_closed"})
	var emitted [][]byte
	var closed []string
	runtime := streamRuntime{
		read: func(string) (pluginapi.HostModelStreamReadResponse, error) {
			return pluginapi.HostModelStreamReadResponse{}, fmt.Errorf("transport reset by peer")
		},
		closeUp: func(string) {},
		emit: func(_ string, payload []byte) error {
			emitted = append(emitted, append([]byte(nil), payload...))
			return nil
		},
		close: func(_ string, message string) { closed = append(closed, message) },
	}
	if err := finishStream(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)}, StreamID: "outer"}, "stream-0", runtime); err != nil {
		t.Fatal(err)
	}
	if len(emitted) != 1 || !strings.Contains(string(emitted[0]), "transport reset by peer") {
		t.Fatalf("emitted error = %#v", emitted)
	}
	if len(closed) != 1 || closed[0] != "" {
		t.Fatalf("close messages = %#v", closed)
	}
}

func TestFinishStreamConvertsEmptyOrEventOnlyCloseToSSEError(t *testing.T) {
	for _, test := range []struct {
		name   string
		chunks []pluginapi.HostModelStreamReadResponse
	}{
		{name: "empty", chunks: []pluginapi.HostModelStreamReadResponse{{Done: true}}},
		{name: "event only", chunks: []pluginapi.HostModelStreamReadResponse{{Payload: []byte("event: response.created")}, {Done: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"gpt-test"}, PostOutputCapacity: "fail_closed"})
			host := &fakeStreamHost{streams: [][]pluginapi.HostModelStreamReadResponse{test.chunks}}
			if err := finishStream(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)}, StreamID: "outer"}, "stream-0", host.runtime()); err != nil {
				t.Fatal(err)
			}
			joined := string(bytes.Join(host.emitted, nil))
			if !strings.Contains(joined, "upstream stream closed before first payload") || !strings.Contains(joined, `"code":"upstream_error"`) {
				t.Fatalf("emitted stream = %q", joined)
			}
			if len(host.closedWith) != 1 || host.closedWith[0] != "" {
				t.Fatalf("close messages = %#v", host.closedWith)
			}
		})
	}
}

func TestRewriteCapacitySSEFrameHandlesMessageOnlyMultilineData(t *testing.T) {
	raw := []byte("event: response.failed\n" +
		"data: {\"type\":\"response.failed\",\n" +
		"data: \"response\":{\"error\":{\"message\":\"Selected model is at capacity\"}}}\n\n")
	rewritten := rewriteCapacitySSEFrame(raw)
	frames := parseSSEFrames(rewritten)
	if len(frames) != 1 {
		t.Fatalf("rewritten frames = %#v, raw = %q", frames, rewritten)
	}
	errorValue := frames[0].JSON["response"].(map[string]any)["error"].(map[string]any)
	if errorValue["code"] != "server_error" || errorValue["type"] != "server_error" {
		t.Fatalf("message-only error = %#v", errorValue)
	}
	if bytes.Count(rewritten, []byte("data:")) != 1 {
		t.Fatalf("multiline data was not collapsed: %q", rewritten)
	}
}

func TestFinishStreamTextOnlyRewritesStructuredTerminalErrorInFullPath(t *testing.T) {
	currentConfig.Store(pluginConfig{Enabled: true, Provider: "codex", Models: []string{"gpt-test"}, PostOutputCapacity: "text_only"})
	terminal := `{"type":"response.failed","sequence_number":37,"trace_id":"trace-1","response":{"id":"resp-1","status":"failed","error":{"type":"server_is_overloaded","code":"server_is_overloaded","message":"Selected model is at capacity","param":"model"}}}`
	host := &fakeStreamHost{streams: [][]pluginapi.HostModelStreamReadResponse{{
		{Payload: []byte("event: response.output_text.delta")},
		{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"partial"}`)},
		{Error: terminal, Done: true},
	}}}
	if err := finishStream(rpcExecutorRequest{ExecutorRequest: pluginapi.ExecutorRequest{Model: "gpt-test", OriginalRequest: []byte(`{"model":"gpt-test"}`)}, StreamID: "outer"}, "stream-0", host.runtime()); err != nil {
		t.Fatal(err)
	}
	if len(host.emitted) != 2 {
		t.Fatalf("emitted chunks = %#v, want text and terminal error", host.emitted)
	}
	frames := parseSSEFrames(host.emitted[1])
	if len(frames) != 1 || frames[0].EventType != "response.failed" {
		t.Fatalf("terminal frames = %#v", frames)
	}
	payload := frames[0].JSON
	if payload["sequence_number"] != float64(37) || payload["trace_id"] != "trace-1" {
		t.Fatalf("terminal metadata changed: %#v", payload)
	}
	if _, added := payload["status"]; added {
		t.Fatalf("unexpected top-level status added: %#v", payload)
	}
	response, ok := payload["response"].(map[string]any)
	if !ok || response["id"] != "resp-1" || response["status"] != "failed" {
		t.Fatalf("response metadata changed: %#v", payload["response"])
	}
	errorValue, ok := response["error"].(map[string]any)
	if !ok || errorValue["code"] != "server_error" || errorValue["type"] != "server_error" || errorValue["message"] != "Selected model is at capacity" || errorValue["param"] != "model" {
		t.Fatalf("rewritten error = %#v", response["error"])
	}
	if len(host.closedWith) != 1 || host.closedWith[0] != "" {
		t.Fatalf("close messages = %#v, want clean close", host.closedWith)
	}
}
