package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseAndClassifyOverload(t *testing.T) {
	payload := []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\"}}\n\n" + "event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"server_is_overloaded\"}}\n\n")
	frames := parseSSEFrames(payload)
	if len(frames) != 2 || !frameIsOverload(frames[1]) {
		t.Fatalf("frames = %#v", frames)
	}
}

func TestSSEDecoderCarriesFramesAcrossChunks(t *testing.T) {
	decoder := &sseDecoder{}
	part := []byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"server_is_overloaded\"}}\n")
	if got := decoder.feed(part, false); len(got) != 0 {
		t.Fatalf("incomplete frame was emitted: %#v", got)
	}
	got := decoder.feed([]byte("\n"), false)
	if len(got) != 1 || !frameIsOverload(got[0]) {
		t.Fatalf("decoded frames = %#v", got)
	}
}

func TestTextOnlyStateCollectsPartialOutputAndResponseID(t *testing.T) {
	state := attemptState{}
	for _, raw := range []string{`event: response.created
data: {"type":"response.created","response":{"id":"r1"}}

`, `event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"hello "}

`, `event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"world"}

`} {
		for _, frame := range parseSSEFrames([]byte(raw)) {
			observeFrame(&state, frame)
		}
	}
	if state.ResponseID != "r1" || !state.Generated || state.ToolCall || state.Partial.String() != "hello world" {
		t.Fatalf("state = %#v", state)
	}
}

func TestToolCallIsDetected(t *testing.T) {
	state := attemptState{}
	for _, frame := range parseSSEFrames([]byte("event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\"}}\n\n")) {
		observeFrame(&state, frame)
	}
	if !state.ToolCall {
		t.Fatal("tool call was not detected")
	}
}

func TestSideEffectingToolVariantsAreDetected(t *testing.T) {
	for _, typ := range []string{"custom_tool_call", "tool_search_call", "mcp_call", "local_shell_call", "apply_patch"} {
		state := attemptState{}
		frame := sseFrame{EventType: "response.output_item.added", JSON: map[string]any{"item": map[string]any{"type": typ}}}
		observeFrame(&state, frame)
		if !state.ToolCall {
			t.Fatalf("tool variant %q was not detected", typ)
		}
	}
}

func TestReasoningOnlyOutputIsNotSafeTextContinuation(t *testing.T) {
	state := attemptState{}
	for _, frame := range parseSSEFrames([]byte("event: response.reasoning.delta\ndata: {\"type\":\"response.reasoning.delta\",\"delta\":\"private\"}\n\n")) {
		observeFrame(&state, frame)
	}
	if !state.Generated || state.TextOutput || state.Partial.Len() != 0 {
		t.Fatalf("reasoning state = %#v", state)
	}
}

func TestBuildContinuationBodyAppendsAssistantAndUserMessages(t *testing.T) {
	body, err := buildContinuationBody([]byte(`{"model":"gpt-5.5","input":[{"type":"message","role":"user","content":"hi"}],"stream":true}`), "partial", "continue")
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	items, ok := payload["input"].([]any)
	if !ok || len(items) != 3 {
		t.Fatalf("input = %#v", payload["input"])
	}
	if got := items[1].(map[string]any)["role"]; got != "assistant" {
		t.Fatalf("assistant role = %v", got)
	}
	if got := items[2].(map[string]any)["content"]; got != "continue" {
		t.Fatalf("prompt = %v", got)
	}
}

func TestBuildContinuationRejectsEmptyPartial(t *testing.T) {
	if _, err := buildContinuationBody([]byte(`{"input":[]}`), "", "continue"); err == nil {
		t.Fatal("expected empty partial error")
	}
}

func TestRewriteResponseIDKeepsSSEShape(t *testing.T) {
	raw := []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"new\"}}\n\n")
	got := string(rewriteResponseID(raw, "old"))
	if !strings.Contains(got, `"id":"old"`) {
		t.Fatalf("rewritten = %s", got)
	}
}
