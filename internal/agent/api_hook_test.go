package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/fantasy"
	"github.com/charmbracelet/crush/internal/agent/tools"
	"github.com/stretchr/testify/require"
)

type apiHookTestModel struct {
	generateResp      *fantasy.Response
	generateErr       error
	stream            fantasy.StreamResponse
	streamErr         error
	generateObjectErr error
	streamObjectErr   error
}

func (m *apiHookTestModel) Provider() string { return "test-provider" }
func (m *apiHookTestModel) Model() string    { return "test-model" }

func (m *apiHookTestModel) Generate(context.Context, fantasy.Call) (*fantasy.Response, error) {
	return m.generateResp, m.generateErr
}

func (m *apiHookTestModel) Stream(context.Context, fantasy.Call) (fantasy.StreamResponse, error) {
	return m.stream, m.streamErr
}

func (m *apiHookTestModel) GenerateObject(context.Context, fantasy.ObjectCall) (*fantasy.ObjectResponse, error) {
	return nil, m.generateObjectErr
}

func (m *apiHookTestModel) StreamObject(context.Context, fantasy.ObjectCall) (fantasy.ObjectStreamResponse, error) {
	return nil, m.streamObjectErr
}

func TestAPIHookGenerateWritesJSONL(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	model := wrapLanguageModelWithAPILogging(&apiHookTestModel{
		generateResp: &fantasy.Response{
			Content:      fantasy.ResponseContent{fantasy.TextContent{Text: "hello world"}},
			FinishReason: fantasy.FinishReasonStop,
			Usage:        fantasy.Usage{InputTokens: 5, OutputTokens: 2, TotalTokens: 7},
		},
	}, dataDir)

	ctx := context.WithValue(t.Context(), tools.SessionIDContextKey, "sess-generate")
	ctx = WithAPILogMetadata(ctx, APILogMetadata{SessionID: "sess-generate", Kind: "agent_turn"})
	_, err := model.Generate(ctx, fantasy.Call{Prompt: fantasy.Prompt{fantasy.NewUserMessage("hi")}})
	require.NoError(t, err)

	record := readSingleAPIHookRecord(t, filepath.Join(dataDir, "logs", "llm", "sess-generate.jsonl"))
	require.Equal(t, "crush", record["agent"])
	require.Equal(t, "sess-generate", record["session_id"])
	require.Equal(t, "agent_turn", record["kind"])
	require.Equal(t, "test-provider", record["provider"])
	require.Equal(t, "test-model", record["model"])
	require.Equal(t, false, record["stream"])
	require.Equal(t, true, record["complete"])

	request := record["request"].(map[string]any)
	messages := request["messages"].([]any)
	firstMessage := messages[0].(map[string]any)
	require.Equal(t, string(fantasy.MessageRoleUser), firstMessage["role"])
	firstContent := firstMessage["content"].([]any)[0].(map[string]any)
	require.Equal(t, "text", firstContent["type"])
	require.Equal(t, "hi", firstContent["text"])

	response := record["response"].(map[string]any)
	content := response["content"].([]any)[0].(map[string]any)
	require.Equal(t, "text", content["type"])
	require.Equal(t, "hello world", content["text"])
	require.Equal(t, string(fantasy.FinishReasonStop), response["stop_reason"])
}

func TestAPIHookStreamWritesIncompleteRecordWhenConsumerStopsEarly(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	model := wrapLanguageModelWithAPILogging(&apiHookTestModel{
		stream: func(yield func(fantasy.StreamPart) bool) {
			if !yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeTextStart, ID: "1"}) {
				return
			}
			if !yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeTextDelta, ID: "1", Delta: "partial"}) {
				return
			}
			if !yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeFinish, FinishReason: fantasy.FinishReasonStop}) {
				return
			}
		},
	}, dataDir)

	ctx := WithAPILogMetadata(t.Context(), APILogMetadata{
		SessionID: "sess-stream-stop",
		Kind:      "agent_turn",
	})
	stream, err := model.Stream(ctx, fantasy.Call{Prompt: fantasy.Prompt{fantasy.NewUserMessage("hi")}})
	require.NoError(t, err)

	count := 0
	for part := range stream {
		count++
		require.Equal(t, fantasy.StreamPartTypeTextStart, part.Type)
		break
	}
	require.Equal(t, 1, count)

	record := readSingleAPIHookRecord(t, filepath.Join(dataDir, "logs", "llm", "sess-stream-stop.jsonl"))
	require.Equal(t, true, record["stream"])
	require.Equal(t, false, record["complete"])
	require.Equal(t, "stream consumer stopped before finish", record["error"])
}

func TestAPIHookStreamUsesWorkspaceDataDirNotProcessCWD(t *testing.T) {
	dataDir := t.TempDir()
	otherDir := t.TempDir()
	oldWD, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(otherDir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(oldWD))
	})

	model := wrapLanguageModelWithAPILogging(&apiHookTestModel{
		stream: func(yield func(fantasy.StreamPart) bool) {
			yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeError, Error: errors.New("stream blew up")})
		},
	}, dataDir)

	ctx := context.WithValue(t.Context(), tools.SessionIDContextKey, "sess-path")
	ctx = WithAPILogMetadata(ctx, APILogMetadata{SessionID: "sess-path", Kind: "agent_turn"})
	stream, err := model.Stream(ctx, fantasy.Call{Prompt: fantasy.Prompt{fantasy.NewUserMessage("hi")}})
	require.NoError(t, err)
	for range stream {
	}

	wantPath := filepath.Join(dataDir, "logs", "llm", "sess-path.jsonl")
	record := readSingleAPIHookRecord(t, wantPath)
	require.Equal(t, "stream blew up", record["error"])

	_, err = os.Stat(filepath.Join(otherDir, "logs", "llm", "sess-path.jsonl"))
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}

func readSingleAPIHookRecord(t *testing.T, path string) map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 1)

	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &record))
	return record
}
