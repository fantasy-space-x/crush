package tools

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"charm.land/fantasy"
	"github.com/charmbracelet/crush/internal/shell"
)

const (
	JobKillToolName       = "job_kill"
	DefaultJobKillTimeout = 5 * time.Second
)

//go:embed job_kill.md
var jobKillDescription string

type JobKillParams struct {
	ShellID string `json:"shell_id" description:"The ID of the background shell to terminate"`
}

type JobKillResponseMetadata struct {
	ShellID     string `json:"shell_id"`
	Command     string `json:"command"`
	Description string `json:"description"`
	TimedOut    bool   `json:"timed_out,omitempty"`
}

func NewJobKillTool() fantasy.AgentTool {
	return fantasy.NewAgentTool(
		JobKillToolName,
		jobKillDescription,
		func(ctx context.Context, params JobKillParams, call fantasy.ToolCall) (fantasy.ToolResponse, error) {
			if params.ShellID == "" {
				return fantasy.NewTextErrorResponse("missing shell_id"), nil
			}

			bgManager := shell.GetBackgroundShellManager()

			bgShell, ok := bgManager.Get(params.ShellID)
			if !ok {
				return fantasy.NewTextErrorResponse(fmt.Sprintf("background shell not found: %s", params.ShellID)), nil
			}

			metadata := JobKillResponseMetadata{
				ShellID:     params.ShellID,
				Command:     bgShell.Command,
				Description: bgShell.Description,
			}

			killCtx, cancel := context.WithTimeout(ctx, DefaultJobKillTimeout)
			defer cancel()

			err := bgManager.KillContext(killCtx, params.ShellID)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					metadata.TimedOut = true
					result := fmt.Sprintf("Terminate request sent to background shell %s, but it did not finish within %s. Ignoring the remaining kill result.", params.ShellID, DefaultJobKillTimeout)
					return fantasy.WithResponseMetadata(fantasy.NewTextResponse(result), metadata), nil
				}
				return fantasy.NewTextErrorResponse(err.Error()), nil
			}

			result := fmt.Sprintf("Background shell %s terminated successfully", params.ShellID)
			return fantasy.WithResponseMetadata(fantasy.NewTextResponse(result), metadata), nil
		},
	)
}
