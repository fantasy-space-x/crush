package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/crush/internal/config"
	crushlog "github.com/charmbracelet/crush/internal/log"
	"github.com/charmbracelet/crush/internal/server"
	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"
)

var serverHost string
var serverHostname string
var serverPort int
var serverBaseURL string
var serverAPIKey string
var serverModel string
var serverHistoryDir string
var serverLogOutput string

func init() {
	serverCmd.Flags().StringVarP(&serverHost, "host", "H", server.DefaultHost(), "Server host (TCP or Unix socket)")
	serverCmd.Flags().StringVar(&serverHostname, "hostname", "0.0.0.0", "TCP hostname to bind when --port is set")
	serverCmd.Flags().IntVarP(&serverPort, "port", "p", 0, "TCP port to bind")
	serverCmd.Flags().StringVar(&serverBaseURL, "base-url", "", "Runtime provider base URL")
	serverCmd.Flags().StringVar(&serverAPIKey, "api-key", "", "Runtime provider API key")
	serverCmd.Flags().StringVar(&serverModel, "model", "", "Runtime model, optionally in provider/model format")
	serverCmd.Flags().StringVar(&serverHistoryDir, "history-dir", "", "Directory for server-created workspace history")
	serverCmd.Flags().StringVar(&serverLogOutput, "log-output", "auto", "Mirror server logs to auto, file, stderr, or stdout")
	rootCmd.AddCommand(serverCmd)
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the Crush server",
	RunE: func(cmd *cobra.Command, _ []string) error {
		dataDir, err := serverDataDir(cmd)
		if err != nil {
			return err
		}
		debug, err := cmd.Flags().GetBool("debug")
		if err != nil {
			return fmt.Errorf("failed to get debug flag: %v", err)
		}
		overrides, err := serverRuntimeOverrides(dataDir)
		if err != nil {
			return err
		}

		cfg, err := config.Load(config.GlobalWorkspaceDir(), dataDir, debug, config.WithRuntimeOverrides(overrides))
		if err != nil {
			return fmt.Errorf("failed to load configuration: %v", err)
		}

		listenHost, err := serverListenHost(cmd)
		if err != nil {
			return err
		}
		hostURL, err := server.ParseHostURL(listenHost)
		if err != nil {
			return fmt.Errorf("invalid server host: %v", err)
		}

		logFile := filepath.Join(config.GlobalCacheDir(), "server-"+safeHostName(hostURL), "crush.log")

		logWriters, err := serverLogWriters(serverLogOutput)
		if err != nil {
			return err
		}
		crushlog.Setup(logFile, debug, logWriters...)

		srv := server.NewServer(cfg, hostURL.Scheme, hostURL.Host)
		srv.SetLogger(slog.Default())
		slog.Info("Starting Crush server", serverStartupAttrs(cfg, listenHost, logFile, debug, overrides)...)

		errch := make(chan error, 1)
		sigch := make(chan os.Signal, 1)
		sigs := []os.Signal{os.Interrupt}
		sigs = append(sigs, addSignals(sigs)...)
		signal.Notify(sigch, sigs...)

		go func() {
			errch <- srv.ListenAndServe()
		}()

		select {
		case <-sigch:
			slog.Info("Received interrupt signal...")
		case err = <-errch:
			if err != nil && !errors.Is(err, server.ErrServerClosed) {
				_ = srv.Close()
				slog.Error("Server error", "error", err)
				return fmt.Errorf("server error: %v", err)
			}
		}

		if errors.Is(err, server.ErrServerClosed) {
			return nil
		}

		ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
		defer cancel()

		slog.Info("Shutting down...")

		if err := srv.Shutdown(ctx); err != nil {
			slog.Error("Failed to shutdown server", "error", err)
			return fmt.Errorf("failed to shutdown server: %v", err)
		}

		return nil
	},
}

func serverStartupAttrs(
	store *config.ConfigStore,
	listenHost, logFile string,
	debug bool,
	overrides config.RuntimeOverrides,
) []any {
	cfg := store.Config()
	return []any{
		"host", listenHost,
		"data_dir", cfg.Options.DataDirectory,
		"log_file", logFile,
		"log_output", serverLogOutput,
		"debug", debug,
		"runtime_model", runtimeModelLabel(overrides.Model),
		"runtime_base_url", baseURLLabel(overrides.Model.BaseURL),
		"runtime_api_key", setStatus(overrides.Model.APIKey),
	}
}

func serverLogWriters(output string) ([]io.Writer, error) {
	switch output {
	case "auto":
		if term.IsTerminal(os.Stderr.Fd()) {
			return []io.Writer{os.Stderr}, nil
		}
		return nil, nil
	case "file":
		return nil, nil
	case "stderr":
		return []io.Writer{os.Stderr}, nil
	case "stdout":
		return []io.Writer{os.Stdout}, nil
	default:
		return nil, fmt.Errorf("invalid --log-output %q; use auto, file, stderr, or stdout", output)
	}
}

func runtimeModelLabel(model config.RuntimeModelOverride) string {
	if model.Model == "" {
		return "none"
	}
	if model.Provider == "" {
		return model.Model
	}
	return model.Provider + "/" + model.Model
}

func baseURLLabel(value string) string {
	if value == "" {
		return "none"
	}
	return sanitizeBaseURL(value)
}

func sanitizeBaseURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.User == nil {
		return value
	}
	parsed.User = url.User("redacted")
	return parsed.String()
}

func setStatus(value string) string {
	if value == "" {
		return "unset"
	}
	return "set"
}

func serverDataDir(cmd *cobra.Command) (string, error) {
	dataDir, err := cmd.Flags().GetString("data-dir")
	if err != nil {
		return "", fmt.Errorf("failed to get data directory: %v", err)
	}
	if serverHistoryDir == "" {
		return absoluteFlagPath(dataDir)
	}
	if dataDir != "" {
		return "", fmt.Errorf("--history-dir cannot be used with --data-dir")
	}
	return absoluteFlagPath(serverHistoryDir)
}

func absoluteFlagPath(path string) (string, error) {
	if path == "" || filepath.IsAbs(path) {
		return path, nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to resolve path %q: %v", path, err)
	}
	return abs, nil
}

func serverRuntimeOverrides(dataDir string) (config.RuntimeOverrides, error) {
	model, err := serverRuntimeModel()
	if err != nil {
		return config.RuntimeOverrides{}, err
	}
	return config.RuntimeOverrides{
		DataDirectory: dataDir,
		Model:         model,
	}, nil
}

func serverRuntimeModel() (config.RuntimeModelOverride, error) {
	if serverModel == "" {
		if serverBaseURL != "" || serverAPIKey != "" {
			return config.RuntimeModelOverride{}, fmt.Errorf("--model is required when --base-url or --api-key is set")
		}
		return config.RuntimeModelOverride{}, nil
	}
	provider, model, err := parseRuntimeModel(serverModel)
	if err != nil {
		return config.RuntimeModelOverride{}, err
	}
	return config.RuntimeModelOverride{
		Provider: provider,
		Model:    model,
		BaseURL:  serverBaseURL,
		APIKey:   serverAPIKey,
	}, nil
}

func parseRuntimeModel(model string) (string, string, error) {
	provider, modelID, ok := strings.Cut(model, "/")
	if !ok {
		return "", model, nil
	}
	if provider == "" || modelID == "" {
		return "", "", fmt.Errorf("invalid --model %q; use model or provider/model", model)
	}
	return provider, modelID, nil
}

func serverListenHost(cmd *cobra.Command) (string, error) {
	if serverPort == 0 {
		if cmd.Flags().Changed("hostname") {
			return "", fmt.Errorf("--hostname requires --port")
		}
		return serverHost, nil
	}
	if serverPort < 0 || serverPort > 65535 {
		return "", fmt.Errorf("invalid --port %d", serverPort)
	}
	if cmd.Flags().Changed("host") {
		return "", fmt.Errorf("--host cannot be used with --port")
	}
	return "tcp://" + net.JoinHostPort(serverHostname, strconv.Itoa(serverPort)), nil
}
