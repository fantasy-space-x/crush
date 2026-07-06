package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"charm.land/fantasy"
	"github.com/charmbracelet/crush/internal/agent/tools"
)

type apiLogMetadataKey struct{}

// APILogMetadata carries per-request metadata for LLM JSONL logging.
type APILogMetadata struct {
	SessionID string
	RunID     string
	MessageID string
	AgentName string
	Kind      string
	// WorkingDir is only used for request JSONL storage.
	WorkingDir string
}

// WithAPILogMetadata returns ctx tagged with metadata for the API hook logger.
func WithAPILogMetadata(ctx context.Context, metadata APILogMetadata) context.Context {
	return context.WithValue(ctx, apiLogMetadataKey{}, metadata)
}

func apiLogMetadataFromContext(ctx context.Context) APILogMetadata {
	metadata, _ := ctx.Value(apiLogMetadataKey{}).(APILogMetadata)
	if metadata.SessionID == "" {
		metadata.SessionID = tools.GetSessionFromContext(ctx)
	}
	if metadata.RunID == "" {
		metadata.RunID = RunIDFromContext(ctx)
	}
	if metadata.MessageID == "" {
		metadata.MessageID = tools.GetMessageFromContext(ctx)
	}
	if metadata.Kind == "" {
		metadata.Kind = "agent_turn"
	}
	return metadata
}

type llmJSONLLogger struct {
	mu sync.Map
}

func newLLMJSONLLogger() *llmJSONLLogger {
	return &llmJSONLLogger{}
}

func (l *llmJSONLLogger) lock(path string) func() {
	mu, _ := l.mu.LoadOrStore(path, &sync.Mutex{})
	mutex := mu.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}

func (l *llmJSONLLogger) recordPath(metadata APILogMetadata) string {
	return filepath.Join(metadata.WorkingDir, ".crush", metadata.SessionID+".jsonl")
}

func (l *llmJSONLLogger) write(metadata APILogMetadata, record apiHookRecord) {
	if l == nil || metadata.WorkingDir == "" || record.SessionID == "" {
		return
	}

	path := l.recordPath(metadata)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		slog.Error("Failed to create LLM log directory", "path", filepath.Dir(path), "error", err)
		return
	}

	line, err := json.Marshal(record)
	if err != nil {
		slog.Error("Failed to marshal LLM log record", "path", path, "error", err)
		return
	}
	line = append(line, '\n')

	unlock := l.lock(path)
	defer unlock()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		slog.Error("Failed to open LLM log file", "path", path, "error", err)
		return
	}
	defer f.Close()

	if _, err := f.Write(line); err != nil {
		slog.Error("Failed to write LLM log record", "path", path, "error", err)
	}
}

type apiHookModel struct {
	inner  fantasy.LanguageModel
	logger *llmJSONLLogger
}

func wrapLanguageModelWithAPILogging(model fantasy.LanguageModel) fantasy.LanguageModel {
	if model == nil {
		return model
	}
	return &apiHookModel{
		inner:  model,
		logger: newLLMJSONLLogger(),
	}
}

func (m *apiHookModel) Provider() string { return m.inner.Provider() }
func (m *apiHookModel) Model() string    { return m.inner.Model() }

func (m *apiHookModel) Generate(ctx context.Context, call fantasy.Call) (*fantasy.Response, error) {
	start := time.Now()
	resp, err := m.inner.Generate(ctx, call)
	metadata := apiLogMetadataFromContext(ctx)
	record := newAPIHookRecord(metadata, m, start, false, sanitizeForJSON(call))
	record.Cancelled = err != nil && ctx.Err() != nil
	if err != nil {
		record.Error = err.Error()
	}
	if resp != nil {
		record.Response = sanitizeResponse(resp)
	}
	record.Complete = err == nil
	m.logger.write(metadata, record)
	return resp, err
}

func (m *apiHookModel) Stream(ctx context.Context, call fantasy.Call) (fantasy.StreamResponse, error) {
	start := time.Now()
	stream, err := m.inner.Stream(ctx, call)
	metadata := apiLogMetadataFromContext(ctx)
	if err != nil {
		record := newAPIHookRecord(metadata, m, start, true, sanitizeForJSON(call))
		record.Error = err.Error()
		record.Cancelled = ctx.Err() != nil
		record.Complete = false
		m.logger.write(metadata, record)
		return nil, err
	}

	return func(yield func(fantasy.StreamPart) bool) {
		acc := newStreamAccumulator()
		stoppedEarly := false
		for part := range stream {
			acc.Add(part)
			if !yield(part) {
				stoppedEarly = true
				break
			}
		}

		record := newAPIHookRecord(metadata, m, start, true, sanitizeForJSON(call))
		record.Response = acc.Response()
		record.Complete = acc.Complete() && !stoppedEarly
		record.Cancelled = ctx.Err() != nil
		if acc.err != "" {
			record.Error = acc.err
		} else if stoppedEarly {
			record.Error = "stream consumer stopped before finish"
		}
		if record.Cancelled && record.Error == "" {
			record.Error = ctx.Err().Error()
		}
		m.logger.write(metadata, record)
	}, nil
}

func (m *apiHookModel) GenerateObject(ctx context.Context, call fantasy.ObjectCall) (*fantasy.ObjectResponse, error) {
	start := time.Now()
	resp, err := m.inner.GenerateObject(ctx, call)
	metadata := apiLogMetadataFromContext(ctx)
	record := newAPIHookRecord(metadata, m, start, false, sanitizeForJSON(call))
	record.Kind = record.Kind + "_object"
	record.Cancelled = err != nil && ctx.Err() != nil
	if err != nil {
		record.Error = err.Error()
	}
	if resp != nil {
		record.Response = sanitizeObjectResponse(resp)
	}
	record.Complete = err == nil
	m.logger.write(metadata, record)
	return resp, err
}

func (m *apiHookModel) StreamObject(ctx context.Context, call fantasy.ObjectCall) (fantasy.ObjectStreamResponse, error) {
	start := time.Now()
	stream, err := m.inner.StreamObject(ctx, call)
	metadata := apiLogMetadataFromContext(ctx)
	if err != nil {
		record := newAPIHookRecord(metadata, m, start, true, sanitizeForJSON(call))
		record.Kind = record.Kind + "_object"
		record.Error = err.Error()
		record.Cancelled = ctx.Err() != nil
		record.Complete = false
		m.logger.write(metadata, record)
		return nil, err
	}

	return func(yield func(fantasy.ObjectStreamPart) bool) {
		acc := newObjectStreamAccumulator()
		stoppedEarly := false
		for part := range stream {
			acc.Add(part)
			if !yield(part) {
				stoppedEarly = true
				break
			}
		}

		record := newAPIHookRecord(metadata, m, start, true, sanitizeForJSON(call))
		record.Kind = record.Kind + "_object"
		record.Response = acc.Response()
		record.Complete = acc.Complete() && !stoppedEarly
		record.Cancelled = ctx.Err() != nil
		if acc.err != "" {
			record.Error = acc.err
		} else if stoppedEarly {
			record.Error = "stream consumer stopped before finish"
		}
		if record.Cancelled && record.Error == "" {
			record.Error = ctx.Err().Error()
		}
		m.logger.write(metadata, record)
	}, nil
}

type apiHookRecord struct {
	Timestamp  time.Time `json:"timestamp"`
	Agent      string    `json:"agent"`
	AgentName  string    `json:"agent_name,omitempty"`
	SessionID  string    `json:"session_id"`
	RunID      string    `json:"run_id,omitempty"`
	MessageID  string    `json:"message_id,omitempty"`
	Kind       string    `json:"kind"`
	Provider   string    `json:"provider"`
	Model      string    `json:"model"`
	Stream     bool      `json:"stream"`
	Complete   bool      `json:"complete"`
	Cancelled  bool      `json:"cancelled,omitempty"`
	DurationMS int64     `json:"duration_ms"`
	Request    any       `json:"request,omitempty"`
	Response   any       `json:"response,omitempty"`
	Error      string    `json:"error,omitempty"`
}

func newAPIHookRecord(metadata APILogMetadata, model fantasy.LanguageModel, start time.Time, stream bool, request any) apiHookRecord {
	return apiHookRecord{
		Timestamp:  start.UTC(),
		Agent:      "crush",
		AgentName:  metadata.AgentName,
		SessionID:  metadata.SessionID,
		RunID:      metadata.RunID,
		MessageID:  metadata.MessageID,
		Kind:       metadata.Kind,
		Provider:   model.Provider(),
		Model:      model.Model(),
		Stream:     stream,
		DurationMS: time.Since(start).Milliseconds(),
		Request:    request,
	}
}

func sanitizeGenericForJSON(v any) any {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err == nil {
		var out any
		if err := json.Unmarshal(b, &out); err == nil {
			return out
		}
	}
	marshalErr := "unknown"
	if err != nil {
		marshalErr = err.Error()
	}
	return map[string]any{
		"string":        fmt.Sprintf("%#v", v),
		"marshal_error": marshalErr,
	}
}

func sanitizeForJSON(v any) any {
	switch call := v.(type) {
	case fantasy.Call:
		return sanitizeCall(call)
	case *fantasy.Call:
		if call == nil {
			return nil
		}
		return sanitizeCall(*call)
	case fantasy.ObjectCall:
		return sanitizeObjectCall(call)
	case *fantasy.ObjectCall:
		if call == nil {
			return nil
		}
		return sanitizeObjectCall(*call)
	default:
		return sanitizeGenericForJSON(v)
	}
}

func sanitizeCall(call fantasy.Call) any {
	return map[string]any{
		"messages":         sanitizePrompt(call.Prompt),
		"decode_params":    sanitizeDecodeParams(call.MaxOutputTokens, call.Temperature, call.TopP, call.TopK, call.PresencePenalty, call.FrequencyPenalty),
		"tools":            sanitizeGenericForJSON(call.Tools),
		"tool_choice":      call.ToolChoice,
		"provider_options": sanitizeGenericForJSON(call.ProviderOptions),
	}
}

func sanitizeObjectCall(call fantasy.ObjectCall) any {
	return map[string]any{
		"messages":           sanitizePrompt(call.Prompt),
		"schema":             sanitizeGenericForJSON(call.Schema),
		"schema_name":        call.SchemaName,
		"schema_description": call.SchemaDescription,
		"decode_params":      sanitizeDecodeParams(call.MaxOutputTokens, call.Temperature, call.TopP, call.TopK, call.PresencePenalty, call.FrequencyPenalty),
		"provider_options":   sanitizeGenericForJSON(call.ProviderOptions),
	}
}

func sanitizeDecodeParams(maxOutputTokens *int64, temperature *float64, topP *float64, topK *int64, presencePenalty *float64, frequencyPenalty *float64) map[string]any {
	return map[string]any{
		"max_output_tokens": maxOutputTokens,
		"temperature":       temperature,
		"top_p":             topP,
		"top_k":             topK,
		"presence_penalty":  presencePenalty,
		"frequency_penalty": frequencyPenalty,
	}
}

func sanitizePrompt(prompt fantasy.Prompt) []any {
	messages := make([]any, 0, len(prompt))
	for _, msg := range prompt {
		entry := map[string]any{
			"role":    msg.Role,
			"content": sanitizeMessageParts(msg.Content),
		}
		messages = append(messages, entry)
	}
	return messages
}

func sanitizeMessageParts(parts []fantasy.MessagePart) []any {
	out := make([]any, 0, len(parts))
	for _, part := range parts {
		switch p := part.(type) {
		case fantasy.TextPart:
			out = append(out, map[string]any{
				"type": "text",
				"text": p.Text,
			})
		default:
			out = append(out, sanitizeGenericForJSON(part))
		}
	}
	return out
}

func sanitizeResponse(resp *fantasy.Response) any {
	if resp == nil {
		return nil
	}
	return map[string]any{
		"content":           sanitizeResponseContent(resp.Content),
		"stop_reason":       resp.FinishReason,
		"usage":             sanitizeForJSON(resp.Usage),
		"warnings":          sanitizeForJSON(resp.Warnings),
		"provider_metadata": sanitizeForJSON(resp.ProviderMetadata),
	}
}

func sanitizeResponseContent(content fantasy.ResponseContent) []any {
	out := make([]any, 0, len(content))
	for _, part := range content {
		switch p := part.(type) {
		case fantasy.TextContent:
			if p.Text != "" {
				out = append(out, textResponsePart(p.Text, sanitizeForJSON(p.ProviderMetadata)))
			}
		case fantasy.ReasoningContent:
			if p.Text != "" {
				reasoning := map[string]any{
					"type": "reasoning",
					"text": p.Text,
				}
				if metadata := sanitizeForJSON(p.ProviderMetadata); metadata != nil {
					reasoning["provider_metadata"] = metadata
				}
				out = append(out, reasoning)
			}
		case fantasy.FileContent:
			out = append(out, map[string]any{
				"type":              "file",
				"media_type":        p.MediaType,
				"data":              p.Data,
				"provider_metadata": sanitizeForJSON(p.ProviderMetadata),
			})
		case fantasy.SourceContent:
			out = append(out, map[string]any{
				"type":              "source",
				"source_type":       p.SourceType,
				"id":                p.ID,
				"url":               p.URL,
				"title":             p.Title,
				"media_type":        p.MediaType,
				"filename":          p.Filename,
				"provider_metadata": sanitizeForJSON(p.ProviderMetadata),
			})
		case fantasy.ToolCallContent:
			out = append(out, map[string]any{
				"type":              "tool_call",
				"id":                p.ToolCallID,
				"name":              p.ToolName,
				"input":             p.Input,
				"provider_executed": p.ProviderExecuted,
				"provider_metadata": sanitizeForJSON(p.ProviderMetadata),
				"invalid":           p.Invalid,
				"validation_error": func() string {
					if p.ValidationError == nil {
						return ""
					}
					return p.ValidationError.Error()
				}(),
			})
		case fantasy.ToolResultContent:
			out = append(out, map[string]any{
				"type":              "tool_result",
				"id":                p.ToolCallID,
				"name":              p.ToolName,
				"result":            sanitizeForJSON(p.Result),
				"client_metadata":   p.ClientMetadata,
				"provider_executed": p.ProviderExecuted,
				"provider_metadata": sanitizeForJSON(p.ProviderMetadata),
				"stop_turn":         p.StopTurn,
			})
		default:
			out = append(out, sanitizeGenericForJSON(part))
		}
	}
	return out
}

func textResponsePart(text string, providerMetadata any) map[string]any {
	part := map[string]any{
		"type": "text",
		"text": text,
	}
	if providerMetadata != nil {
		part["provider_metadata"] = providerMetadata
	}
	return part
}

func sanitizeObjectResponse(resp *fantasy.ObjectResponse) any {
	if resp == nil {
		return nil
	}
	return map[string]any{
		"content":           objectResponseContent(resp.RawText, sanitizeForJSON(resp.Object)),
		"stop_reason":       resp.FinishReason,
		"usage":             sanitizeForJSON(resp.Usage),
		"warnings":          sanitizeForJSON(resp.Warnings),
		"provider_metadata": sanitizeForJSON(resp.ProviderMetadata),
	}
}

func objectResponseContent(rawText string, object any) []any {
	content := []any{}
	if rawText != "" {
		content = append(content, textResponsePart(rawText, nil))
	}
	if object != nil {
		content = append(content, map[string]any{
			"type":   "object",
			"object": object,
		})
	}
	return content
}

type streamAccumulator struct {
	textOrder      []string
	textParts      map[string]*strings.Builder
	reasoningOrder []string
	reasoningParts map[string]*strings.Builder
	toolCalls      map[string]map[string]any
	toolOrder      []string
	toolResults    []any
	sources        []any
	warnings       []any
	finishReason   fantasy.FinishReason
	usage          fantasy.Usage
	providerMeta   any
	err            string
	sawFinish      bool
}

func newStreamAccumulator() *streamAccumulator {
	return &streamAccumulator{
		textParts:      make(map[string]*strings.Builder),
		reasoningParts: make(map[string]*strings.Builder),
		toolCalls:      make(map[string]map[string]any),
	}
}

func (a *streamAccumulator) Add(part fantasy.StreamPart) {
	switch part.Type {
	case fantasy.StreamPartTypeTextStart:
		if _, ok := a.textParts[part.ID]; !ok {
			a.textParts[part.ID] = &strings.Builder{}
			a.textOrder = append(a.textOrder, part.ID)
		}
	case fantasy.StreamPartTypeTextDelta:
		if _, ok := a.textParts[part.ID]; !ok {
			a.textParts[part.ID] = &strings.Builder{}
			a.textOrder = append(a.textOrder, part.ID)
		}
		a.textParts[part.ID].WriteString(part.Delta)
	case fantasy.StreamPartTypeReasoningStart:
		if _, ok := a.reasoningParts[part.ID]; !ok {
			a.reasoningParts[part.ID] = &strings.Builder{}
			a.reasoningOrder = append(a.reasoningOrder, part.ID)
		}
	case fantasy.StreamPartTypeReasoningDelta:
		if _, ok := a.reasoningParts[part.ID]; !ok {
			a.reasoningParts[part.ID] = &strings.Builder{}
			a.reasoningOrder = append(a.reasoningOrder, part.ID)
		}
		a.reasoningParts[part.ID].WriteString(part.Delta)
	case fantasy.StreamPartTypeToolInputStart:
		tc := a.ensureToolCall(part.ID)
		if part.ToolCallName != "" {
			tc["name"] = part.ToolCallName
		}
		tc["input"] = ""
		tc["provider_executed"] = part.ProviderExecuted
	case fantasy.StreamPartTypeToolInputDelta:
		tc := a.ensureToolCall(part.ID)
		input, _ := tc["input"].(string)
		tc["input"] = input + part.Delta
	case fantasy.StreamPartTypeToolInputEnd:
		tc := a.ensureToolCall(part.ID)
		if part.ToolCallName != "" {
			tc["name"] = part.ToolCallName
		}
		if part.ToolCallInput != "" {
			tc["input"] = part.ToolCallInput
		}
	case fantasy.StreamPartTypeToolCall:
		tc := a.ensureToolCall(part.ID)
		if part.ToolCallName != "" {
			tc["name"] = part.ToolCallName
		}
		if part.ToolCallInput != "" {
			tc["input"] = part.ToolCallInput
		}
		tc["provider_executed"] = part.ProviderExecuted
		tc["finished"] = true
	case fantasy.StreamPartTypeToolResult:
		a.toolResults = append(a.toolResults, map[string]any{
			"type":              "tool_result",
			"id":                part.ID,
			"name":              part.ToolCallName,
			"provider_executed": part.ProviderExecuted,
			"provider_metadata": sanitizeForJSON(part.ProviderMetadata),
		})
	case fantasy.StreamPartTypeSource:
		a.sources = append(a.sources, map[string]any{
			"type":              "source",
			"id":                part.ID,
			"source_type":       part.SourceType,
			"url":               part.URL,
			"title":             part.Title,
			"provider_metadata": sanitizeForJSON(part.ProviderMetadata),
		})
	case fantasy.StreamPartTypeWarnings:
		a.warnings = append(a.warnings, sanitizeForJSON(part.Warnings))
	case fantasy.StreamPartTypeError:
		if part.Error != nil {
			a.err = part.Error.Error()
		} else {
			a.err = "stream returned an unspecified error"
		}
	case fantasy.StreamPartTypeFinish:
		a.sawFinish = true
		a.finishReason = part.FinishReason
		a.usage = part.Usage
		if len(part.Warnings) > 0 {
			a.warnings = append(a.warnings, sanitizeForJSON(part.Warnings))
		}
		a.providerMeta = sanitizeForJSON(part.ProviderMetadata)
	}
}

func (a *streamAccumulator) Complete() bool {
	return a.sawFinish && a.err == ""
}

func (a *streamAccumulator) Response() any {
	return map[string]any{
		"content":           a.responseContent(),
		"stop_reason":       a.finishReason,
		"usage":             sanitizeForJSON(a.usage),
		"warnings":          a.warnings,
		"provider_metadata": a.providerMeta,
	}
}

func (a *streamAccumulator) responseContent() []any {
	content := []any{}
	if reasoning := joinBuilders(a.reasoningOrder, a.reasoningParts); reasoning != "" {
		content = append(content, map[string]any{
			"type": "reasoning",
			"text": reasoning,
		})
	}
	if text := joinBuilders(a.textOrder, a.textParts); text != "" {
		content = append(content, textResponsePart(text, nil))
	}
	for _, id := range a.toolOrder {
		content = append(content, a.toolCalls[id])
	}
	content = append(content, a.toolResults...)
	content = append(content, a.sources...)
	return content
}

func (a *streamAccumulator) ensureToolCall(id string) map[string]any {
	if tc, ok := a.toolCalls[id]; ok {
		return tc
	}
	tc := map[string]any{
		"type": "tool_call",
		"id":   id,
	}
	a.toolCalls[id] = tc
	a.toolOrder = append(a.toolOrder, id)
	return tc
}

type objectStreamAccumulator struct {
	object       any
	rawText      strings.Builder
	warnings     []any
	finishReason fantasy.FinishReason
	usage        fantasy.Usage
	providerMeta any
	err          string
	sawFinish    bool
}

func newObjectStreamAccumulator() *objectStreamAccumulator {
	return &objectStreamAccumulator{}
}

func (a *objectStreamAccumulator) Add(part fantasy.ObjectStreamPart) {
	switch part.Type {
	case fantasy.ObjectStreamPartTypeObject:
		a.object = sanitizeForJSON(part.Object)
	case fantasy.ObjectStreamPartTypeTextDelta:
		a.rawText.WriteString(part.Delta)
	case fantasy.ObjectStreamPartTypeError:
		if part.Error != nil {
			a.err = part.Error.Error()
		} else {
			a.err = "object stream returned an unspecified error"
		}
	case fantasy.ObjectStreamPartTypeFinish:
		a.sawFinish = true
		a.finishReason = part.FinishReason
		a.usage = part.Usage
		a.providerMeta = sanitizeForJSON(part.ProviderMetadata)
		if len(part.Warnings) > 0 {
			a.warnings = append(a.warnings, sanitizeForJSON(part.Warnings))
		}
	}
}

func (a *objectStreamAccumulator) Complete() bool {
	return a.sawFinish && a.err == ""
}

func (a *objectStreamAccumulator) Response() any {
	return map[string]any{
		"content":           objectResponseContent(a.rawText.String(), a.object),
		"stop_reason":       a.finishReason,
		"usage":             sanitizeForJSON(a.usage),
		"warnings":          a.warnings,
		"provider_metadata": a.providerMeta,
	}
}

func joinBuilders(order []string, builders map[string]*strings.Builder) string {
	var out strings.Builder
	for _, id := range order {
		if b := builders[id]; b != nil {
			out.WriteString(b.String())
		}
	}
	return out.String()
}

var _ fantasy.LanguageModel = (*apiHookModel)(nil)
