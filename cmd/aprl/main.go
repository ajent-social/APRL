// Package main parses APRL role commands and fails closed without trusted
// production adapters.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/app"
)

var errTrustedAdaptersUnavailable = errors.New("trusted production adapters are unavailable")

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "aprl:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("role required: api, control, or worker")
	}
	role := app.Role(strings.TrimSpace(args[0]))
	if role != app.RoleAPI && role != app.RoleControl && role != app.RoleWorker {
		return errors.New("unknown role; expected api, control, or worker")
	}
	config, err := parseConfig(role, args[1:], stdout, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if err := validateConfig(config); err != nil {
		return err
	}
	// The repository intentionally has no production trust, host-recovery,
	// cancellation, OCI, or provider factory. Do not manufacture one from CLI
	// input or start a partially configured role.
	switch role {
	case app.RoleAPI:
		return fmt.Errorf("api role: %w", errTrustedAdaptersUnavailable)
	case app.RoleControl:
		return fmt.Errorf("control role: %w", errTrustedAdaptersUnavailable)
	case app.RoleWorker:
		return fmt.Errorf("worker role: OCI, provider, and host adapters: %w", errTrustedAdaptersUnavailable)
	default:
		return errors.New("unknown role")
	}
}

func parseConfig(role app.Role, args []string, stdout, stderr io.Writer) (app.Config, error) {
	flags := flag.NewFlagSet("aprl "+string(role), flag.ContinueOnError)
	flags.SetOutput(stderr)
	config := app.Config{
		Role:                  role,
		ListenAddr:            "127.0.0.1:8080",
		StartupTimeout:        30 * time.Second,
		RecoveryTimeout:       20 * time.Second,
		RPCDeadline:           5 * time.Second,
		PollInterval:          time.Second,
		ShutdownTimeout:       10 * time.Second,
		ReadinessTimeout:      3 * time.Second,
		ReadinessPollInterval: 100 * time.Millisecond,
		MaxRequestBodyBytes:   1 << 20,
		DispatchBatch:         16,
		ConsumerName:          "",
	}
	flags.StringVar(&config.ListenAddr, "listen", config.ListenAddr, "health and API bind address")
	flags.DurationVar(&config.StartupTimeout, "startup-timeout", config.StartupTimeout, "maximum startup window")
	flags.DurationVar(&config.RecoveryTimeout, "recovery-timeout", config.RecoveryTimeout, "maximum recovery call")
	flags.DurationVar(&config.RPCDeadline, "rpc-deadline", config.RPCDeadline, "maximum dependency call")
	flags.DurationVar(&config.PollInterval, "poll-interval", config.PollInterval, "bounded control poll interval")
	flags.DurationVar(&config.ShutdownTimeout, "shutdown-timeout", config.ShutdownTimeout, "maximum graceful shutdown")
	flags.DurationVar(&config.ReadinessTimeout, "readiness-timeout", config.ReadinessTimeout, "maximum readiness probe window")
	flags.DurationVar(&config.ReadinessPollInterval, "readiness-poll-interval", config.ReadinessPollInterval, "readiness retry interval")
	flags.Int64Var(&config.MaxRequestBodyBytes, "max-request-bytes", config.MaxRequestBodyBytes, "maximum HTTP request body size")
	flags.IntVar(&config.DispatchBatch, "dispatch-batch", config.DispatchBatch, "maximum items handled per control pass")
	flags.StringVar(&config.ConsumerName, "consumer-name", config.ConsumerName, "durable worker consumer identity")
	if err := flags.Parse(args); err != nil {
		return app.Config{}, err
	}
	if flags.NArg() != 0 {
		return app.Config{}, errors.New("unexpected positional arguments")
	}
	_ = stdout // Reserved for future non-sensitive command output; no adapter details are printed.
	return config, nil
}

func validateConfig(config app.Config) error {
	if _, _, err := net.SplitHostPort(config.ListenAddr); err != nil {
		return errors.New("-listen must be a host:port address")
	}
	if config.Role == app.RoleWorker && strings.TrimSpace(config.ConsumerName) == "" {
		return errors.New("worker role requires -consumer-name")
	}
	if config.StartupTimeout <= 0 || config.StartupTimeout > 5*time.Minute ||
		config.RecoveryTimeout <= 0 || config.RecoveryTimeout > 5*time.Minute ||
		config.RPCDeadline <= 0 || config.RPCDeadline > 30*time.Second ||
		config.PollInterval <= 0 || config.PollInterval > time.Minute ||
		config.ShutdownTimeout <= 0 || config.ShutdownTimeout > time.Minute ||
		config.ReadinessTimeout <= 0 || config.ReadinessTimeout > 30*time.Second ||
		config.ReadinessPollInterval <= 0 || config.ReadinessPollInterval > 30*time.Second ||
		config.MaxRequestBodyBytes <= 0 || config.MaxRequestBodyBytes > 10<<20 ||
		config.DispatchBatch < 1 || config.DispatchBatch > 32 {
		return errors.New("configured bounds are outside the supported finite ranges")
	}
	return nil
}
