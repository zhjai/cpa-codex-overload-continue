package main

import (
	"strings"
	"testing"
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
	if !state.SemanticOutput || !state.Generated || state.ToolCall {
		t.Fatalf("reasoning state = %#v", state)
	}
	tool := sseFrame{EventType: "response.output_item.added", JSON: map[string]any{"item": map[string]any{"type": "computer_call"}}}
	observeFrame(&state, tool)
	if !state.ToolCall {
		t.Fatal("tool call was not detected")
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
	if canRewriteCapacity(attemptState{SemanticOutput: true}, pluginConfig{PostOutputCapacity: "fail_closed"}) {
		t.Fatal("fail_closed rewrote capacity")
	}
	if canRewriteCapacity(attemptState{SemanticOutput: true, ToolCall: true}, pluginConfig{PostOutputCapacity: "text_only"}) {
		t.Fatal("tool turn rewrote capacity")
	}
	if !canRewriteCapacity(attemptState{SemanticOutput: true}, pluginConfig{PostOutputCapacity: "text_only"}) {
		t.Fatal("text-only output was not eligible")
	}
}

func TestPreOutputCapacityFailureRemainsRetryable(t *testing.T) {
	state := attemptState{Overloaded: true}
	if streamCommitted(state) {
		t.Fatal("empty pre-output attempt was marked committed")
	}
	if got := streamErrorResult(state, `{"error":{"code":"server_is_overloaded"}}`, pluginConfig{PostOutputCapacity: "text_only"}); got.ClosePayload != "" {
		t.Fatalf("pre-output close payload = %q, want retryable empty payload", got.ClosePayload)
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
