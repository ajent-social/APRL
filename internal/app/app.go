// Package app assembles APRL's separately supervised HTTP, control, and worker
// roles from explicit trusted dependencies.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/dispatch"
	"github.com/ajent-social/APRL/internal/httpapi"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/processholds"
	"github.com/ajent-social/APRL/internal/queue"
	"github.com/ajent-social/APRL/internal/reconcile"
	"github.com/ajent-social/APRL/internal/results"
	"github.com/ajent-social/APRL/internal/router"
	"github.com/ajent-social/APRL/internal/supervisor"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	maxStartupTimeout  = 5 * time.Minute
	maxRPCDeadline     = 30 * time.Second
	maxPollInterval    = time.Minute
	maxShutdownTimeout = time.Minute
	maxReadinessWindow = 30 * time.Second
	maxRequestBytes    = 10 << 20
	maxDispatchBatch   = 32
	maxCancelPayload   = 4096
	maxWorkerLifetime  = 24 * time.Hour
	maxRedisBlock      = 30 * time.Second
)

var (
	// ErrInvalidConfig reports invalid role configuration or input.
	ErrInvalidConfig = errors.New("invalid app configuration")
	// ErrUnavailable reports a missing or unavailable trusted dependency.
	ErrUnavailable = errors.New("required trusted dependency is unavailable")
	// ErrAlreadyRunning reports a second concurrent call to App.Run.
	ErrAlreadyRunning    = errors.New("app is already running")
	errStartupIncomplete = errors.New("startup recovery is incomplete")
)

// Role identifies one independently supervised APRL process role.
type Role string

const (
	// RoleAPI serves signed webhook and authenticated result endpoints.
	RoleAPI Role = "api"
	// RoleControl routes durable work and reconciles its projections.
	RoleControl Role = "control"
	// RoleWorker consumes admitted jobs and supervises their execution.
	RoleWorker Role = "worker"
)

// Config bounds service startup, calls, polling, health checks, and shutdown.
type Config struct {
	Role                  Role
	ListenAddr            string
	StartupTimeout        time.Duration
	RecoveryTimeout       time.Duration
	RPCDeadline           time.Duration
	PollInterval          time.Duration
	ShutdownTimeout       time.Duration
	ReadinessTimeout      time.Duration
	ReadinessPollInterval time.Duration
	MaxRequestBodyBytes   int64
	DispatchBatch         int
	ConsumerName          string
}

// StartupRecovery inventories durable process holds before mutation or work
// admission and reports whether the current inventory remains trustworthy.
type StartupRecovery interface {
	Recover(context.Context) error
	Ready(context.Context) error
}

// ReadinessProbe checks a trusted role-specific dependency within its caller's
// deadline.
type ReadinessProbe interface {
	Check(context.Context) error
}

// DependencyStatus contains only stable, non-sensitive readiness facts.
type DependencyStatus struct {
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
	Code  string `json:"code,omitempty"`
}

// Readiness is the internal readiness projection returned by App.Ready.
type Readiness struct {
	Role         Role               `json:"role"`
	Ready        bool               `json:"ready"`
	Dependencies []DependencyStatus `json:"dependencies"`
}

// APIDependencies are the trusted inputs used to construct the mutating API
// ingress and authenticated result endpoint.
type APIDependencies struct {
	Pool            *pgxpool.Pool
	Clock           clock.Clock
	RouterConfig    router.Config
	WebhookSecret   []byte
	SupervisorAuth  httpapi.SupervisorAuthenticator
	StartupRecovery StartupRecovery
}

// ControlDependencies are the trusted inputs used to build inbox routing,
// durable outbox dispatch, and reconciliation. Streams is caller-owned.
type ControlDependencies struct {
	Pool            *pgxpool.Pool
	Redis           *redis.Client
	Clock           clock.Clock
	Streams         *queue.Streams
	RouterConfig    router.Config
	Operations      reconcile.OperationReconciler
	Labels          reconcile.LabelOutputter
	ReconcileConfig reconcile.Config
	CancelAck       dispatch.Handler
	NotifyAck       dispatch.Handler
	StartupRecovery StartupRecovery
}

// WorkerDependencies are the trusted inputs used to build one supervisor and
// queue consumer. The immutable envelope is shared by reservation and run.
type WorkerDependencies struct {
	Pool              *pgxpool.Pool
	Redis             *redis.Client
	Clock             clock.Clock
	Streams           *queue.Streams
	Runner            supervisor.Runner
	ResultSubmitter   supervisor.ResultSubmitter
	Holds             *processholds.Store
	SupervisorConfig  supervisor.Config
	ConsumerConfig    queue.ConsumerConfig
	BudgetEnvelope    contracts.BudgetEnvelope
	StartupRecovery   StartupRecovery
	OCIReadiness      ReadinessProbe
	ProviderReadiness ReadinessProbe
	HostReadiness     ReadinessProbe
}

type namedCheck struct {
	name       string
	check      func(context.Context) error
	configured bool
}

// App owns its listener and role loops. Database and Redis clients are supplied
// by the caller and remain caller-owned.
type App struct {
	config   Config
	role     Role
	handler  http.Handler
	checks   []namedCheck
	recovery StartupRecovery
	streams  *queue.Streams
	loop     func(context.Context) error

	startupComplete atomic.Bool
	loopStarted     atomic.Bool
	loopReady       atomic.Bool
	mu              sync.Mutex
	running         bool
	cancel          context.CancelFunc
	done            chan struct{}
	server          *http.Server
}

// NewAPI builds webhook routing and authenticated result handlers. It starts
// no dispatcher, control, or inference loop.
func NewAPI(config Config, deps APIDependencies) (*App, error) {
	if err := config.validate(RoleAPI); err != nil {
		return nil, err
	}
	if nilDependency(deps.Pool) || nilDependency(deps.Clock) || nilDependency(deps.SupervisorAuth) ||
		nilDependency(deps.StartupRecovery) || len(deps.WebhookSecret) == 0 || deps.RouterConfig.MaxTaskBudgetMicroUSD <= 0 || len(deps.RouterConfig.Repositories) == 0 {
		return nil, fmt.Errorf("API requires database, clock, explicit router policy, webhook secret, trusted supervisor auth, and startup recovery: %w", ErrUnavailable)
	}
	pushRouter, err := router.New(deps.Pool, deps.Clock, deps.RouterConfig)
	if err != nil {
		return nil, fmt.Errorf("construct API router: %w", err)
	}
	resultService, err := results.New(deps.Pool, deps.Clock, pushRouter)
	if err != nil {
		return nil, fmt.Errorf("construct result service: %w", err)
	}
	webhookHandler, err := httpapi.NewWebhookHandler(deps.WebhookSecret, deps.Pool, deps.Clock)
	if err != nil {
		return nil, fmt.Errorf("construct webhook handler: %w", err)
	}
	resultHandler, err := httpapi.NewResultsHandler(resultService, deps.SupervisorAuth)
	if err != nil {
		return nil, fmt.Errorf("construct result handler: %w", err)
	}
	app := &App{config: config, role: RoleAPI, recovery: deps.StartupRecovery}
	app.checks = []namedCheck{
		{name: "postgres", check: pgCheck(deps.Pool)},
		{name: "router_policy", configured: true},
		{name: "startup_recovery", check: app.recoveryCheck},
		{name: "supervisor_authenticator", configured: true},
		{name: "webhook_hmac", configured: true},
	}
	app.handler = app.newHandler(map[string]http.Handler{
		"/webhooks/github":  webhookHandler,
		"/internal/results": resultHandler,
	})
	return app, nil
}

// NewControl builds the durable webhook inbox, outbox dispatcher, and
// reconciliation service. It does not construct or infer a queue consumer.
func NewControl(config Config, deps ControlDependencies) (*App, error) {
	if err := config.validate(RoleControl); err != nil {
		return nil, err
	}
	if nilDependency(deps.Pool) || nilDependency(deps.Redis) || nilDependency(deps.Clock) || nilDependency(deps.Streams) ||
		nilDependency(deps.Operations) || nilDependency(deps.Labels) || nilDependency(deps.StartupRecovery) ||
		deps.CancelAck == nil || deps.NotifyAck == nil || deps.RouterConfig.MaxTaskBudgetMicroUSD <= 0 || len(deps.RouterConfig.Repositories) == 0 {
		return nil, fmt.Errorf("control requires database, Redis, streams, explicit router policy, broker, labels, ack adapters, and startup recovery: %w", ErrUnavailable)
	}
	pushRouter, err := router.New(deps.Pool, deps.Clock, deps.RouterConfig)
	if err != nil {
		return nil, fmt.Errorf("construct control router: %w", err)
	}
	service, err := reconcile.New(deps.Pool, deps.Clock, deps.Streams, deps.Operations, deps.StartupRecovery.Ready,
		leases.FenceExpiredTask, deps.Labels, deps.ReconcileConfig)
	if err != nil {
		return nil, fmt.Errorf("construct reconciler: %w", err)
	}
	cancelHandler := makeCancelHandler(deps.CancelAck, config.RPCDeadline)
	notifyHandler := makeBoundedHandler(deps.NotifyAck, config.RPCDeadline)
	dispatcher, err := dispatch.NewDispatcher(deps.Pool, deps.Clock, deps.Streams, map[string]dispatch.Handler{
		"CANCEL": cancelHandler,
		"NOTIFY": notifyHandler,
	})
	if err != nil {
		return nil, fmt.Errorf("construct dispatcher: %w", err)
	}
	app := &App{config: config, role: RoleControl, recovery: deps.StartupRecovery, streams: deps.Streams}
	app.loop = func(ctx context.Context) error {
		return app.runControl(ctx, deps.Pool, pushRouter, dispatcher, service)
	}
	app.checks = []namedCheck{
		{name: "cancel_ack", configured: true},
		{name: "labels", configured: true},
		{name: "notify_ack", configured: true},
		{name: "postgres", check: pgCheck(deps.Pool)},
		{name: "redis", check: redisCheck(deps.Redis)},
		{name: "router_policy", configured: true},
		{name: "startup_recovery", check: app.recoveryCheck},
		{name: "broker_operations", configured: true},
	}
	app.handler = app.newHandler(nil)
	return app, nil
}

// NewWorker builds the durable budget-gated consumer and native process
// supervisor. There is no fallback runner, result submitter, or host verifier.
func NewWorker(config Config, deps WorkerDependencies) (*App, error) {
	if err := config.validate(RoleWorker); err != nil {
		return nil, err
	}
	if nilDependency(deps.Pool) || nilDependency(deps.Redis) || nilDependency(deps.Clock) || nilDependency(deps.Streams) ||
		nilDependency(deps.Runner) || nilDependency(deps.ResultSubmitter) || nilDependency(deps.Holds) || nilDependency(deps.StartupRecovery) ||
		nilDependency(deps.OCIReadiness) || nilDependency(deps.ProviderReadiness) || nilDependency(deps.HostReadiness) ||
		deps.ConsumerConfig.BudgetAdmission != nil || deps.SupervisorConfig.SupervisorIdentity == "" ||
		deps.SupervisorConfig.SupervisorIdentity != deps.ConsumerConfig.SupervisorIdentity ||
		deps.SupervisorConfig.CredentialID == "" || deps.SupervisorConfig.CredentialID != deps.ConsumerConfig.CredentialID ||
		!finiteWorkerConfig(deps.ConsumerConfig, deps.SupervisorConfig, config) {
		return nil, fmt.Errorf("worker requires database, Redis, streams, runner, result submitter, host holds, matching admission identity, envelope, and readiness probes: %w", ErrUnavailable)
	}
	if err := deps.BudgetEnvelope.Validate(); err != nil {
		return nil, fmt.Errorf("worker budget envelope: %w", err)
	}
	worker, err := supervisor.New(deps.Pool, deps.Clock, deps.Runner, deps.ResultSubmitter, deps.Holds, deps.SupervisorConfig)
	if err != nil {
		return nil, fmt.Errorf("construct supervisor: %w", err)
	}
	app := &App{config: config, role: RoleWorker, recovery: deps.StartupRecovery, streams: deps.Streams}
	app.checks = []namedCheck{
		{name: "host_process_inventory", check: deps.HostReadiness.Check},
		{name: "oci_runtime", check: deps.OCIReadiness.Check},
		{name: "postgres", check: pgCheck(deps.Pool)},
		{name: "provider", check: deps.ProviderReadiness.Check},
		{name: "redis", check: redisCheck(deps.Redis)},
		{name: "startup_recovery", check: app.recoveryCheck},
	}
	consumerConfig := deps.ConsumerConfig
	consumerConfig.BudgetAdmission = func(ctx context.Context, lease leases.Lease, job contracts.Job) error {
		if !app.readiness(ctx, false).Ready {
			return fmt.Errorf("worker dependencies are unavailable before budget admission: %w", ErrUnavailable)
		}
		if job.Envelope != deps.BudgetEnvelope {
			return fmt.Errorf("durable job envelope does not match configured immutable worker envelope: %w", queue.ErrInferenceAdmissionRequired)
		}
		_, err := budget.Reserve(ctx, deps.Pool, deps.Clock, lease, deps.BudgetEnvelope)
		return err
	}
	consumer, err := queue.NewConsumer(deps.Pool, deps.Clock, deps.Streams, consumerConfig,
		func(ctx context.Context, lease leases.Lease) error {
			return worker.Run(ctx, lease, deps.BudgetEnvelope)
		})
	if err != nil {
		return nil, fmt.Errorf("construct worker consumer: %w", err)
	}
	app.loop = func(ctx context.Context) error { return app.runWorker(ctx, deps.Streams, consumer, consumerConfig) }
	app.handler = app.newHandler(nil)
	return app, nil
}

// Handler exposes health endpoints for every role and business endpoints only
// for the API role.
func (a *App) Handler() http.Handler {
	if a == nil || a.handler == nil {
		return http.NotFoundHandler()
	}
	return a.handler
}

// Ready probes the current role dependencies with a finite deadline. Raw
// dependency errors never leave the process.
func (a *App) Ready(ctx context.Context) Readiness {
	return a.readiness(ctx, true)
}

// readiness probes configured dependencies and optionally includes this role's
// loop status. Recovery retries omit only loop status so an unavailable loop
// can restart after its backing dependencies recover.
func (a *App) readiness(ctx context.Context, includeLoop bool) Readiness {
	readiness := Readiness{Role: "unavailable", Dependencies: []DependencyStatus{{Name: "app", Code: "unavailable"}}}
	if a == nil || ctx == nil || a.config.ReadinessTimeout <= 0 {
		return readiness
	}
	readiness.Role = a.role
	readiness.Dependencies = nil
	probeCtx, cancel := context.WithTimeout(ctx, a.config.ReadinessTimeout)
	defer cancel()
	readiness.Ready = true
	for _, dependency := range a.checks {
		status := DependencyStatus{Name: dependency.name, Ready: true}
		if dependency.configured {
			status.Code = "configured"
		} else if dependency.check == nil {
			status.Ready = false
			status.Code = "unavailable"
		} else {
			callCtx, callCancel := context.WithTimeout(probeCtx, a.config.RPCDeadline)
			err := dependency.check(callCtx)
			if err == nil {
				err = callCtx.Err()
			}
			callCancel()
			if err != nil {
				status.Ready = false
				status.Code = "unavailable"
			}
		}
		if !status.Ready {
			readiness.Ready = false
		}
		readiness.Dependencies = append(readiness.Dependencies, status)
	}
	if !a.startupComplete.Load() {
		readiness.Ready = false
		setDependency(&readiness, "startup_recovery", false, "not_started")
	}
	if includeLoop && a.loopStarted.Load() && !a.loopReady.Load() {
		readiness.Ready = false
		setDependency(&readiness, string(a.role)+"_loop", false, "unavailable")
	} else if includeLoop && a.loopStarted.Load() {
		setDependency(&readiness, string(a.role)+"_loop", true, "configured")
	}
	sort.Slice(readiness.Dependencies, func(i, j int) bool {
		return readiness.Dependencies[i].Name < readiness.Dependencies[j].Name
	})
	return readiness
}

// Run binds health HTTP, completes bounded startup recovery, validates current
// readiness, then starts only this app's role loop.
func (a *App) Run(ctx context.Context) (result error) {
	if a == nil || ctx == nil || ctx.Err() != nil {
		return ErrInvalidConfig
	}
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return ErrAlreadyRunning
	}
	runCtx, cancel := context.WithCancel(ctx)
	a.running = true
	a.cancel = cancel
	a.done = make(chan struct{})
	done := a.done
	var loopDone chan struct{}
	a.mu.Unlock()
	defer func() {
		cancel()
		a.startupComplete.Store(false)
		a.mu.Lock()
		server := a.server
		a.mu.Unlock()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), a.config.ShutdownTimeout)
		if server != nil {
			if err := server.Shutdown(shutdownCtx); err != nil {
				_ = server.Close()
			}
		}
		if loopDone != nil {
			select {
			case <-loopDone:
			case <-shutdownCtx.Done():
				if result == nil {
					result = fmt.Errorf("%s role loop did not stop within shutdown bound", a.role)
				}
			}
		}
		shutdownCancel()
		a.mu.Lock()
		a.running = false
		a.cancel = nil
		a.server = nil
		if a.done == done {
			a.done = nil
		}
		close(done)
		a.mu.Unlock()
	}()

	listener, err := net.Listen("tcp", a.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen for %s health: %w", a.role, err)
	}
	server := &http.Server{Handler: a.Handler(), ReadHeaderTimeout: a.config.RPCDeadline, IdleTimeout: a.config.ShutdownTimeout}
	a.mu.Lock()
	a.server = server
	a.mu.Unlock()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	startupCtx, startupCancel := context.WithTimeout(runCtx, a.config.StartupTimeout)
	recoveryCtx, recoveryCancel := context.WithTimeout(startupCtx, a.config.RecoveryTimeout)
	err = a.recovery.Recover(recoveryCtx)
	if err == nil {
		err = recoveryCtx.Err()
	}
	recoveryCancel()
	if err != nil {
		if runCtx.Err() != nil {
			startupCancel()
			return nil
		}
		startupCancel()
		return fmt.Errorf("startup recovery: %w", err)
	}
	a.startupComplete.Store(true)
	if err := a.awaitReadiness(startupCtx); err != nil {
		startupCancel()
		return err
	}
	if a.streams != nil {
		if err := a.streams.EnsureGroup(startupCtx); err != nil {
			startupCancel()
			return fmt.Errorf("initialize queue after recovery: %w", err)
		}
	}
	startupCancel()

	var loopErr <-chan error
	if a.loop != nil {
		a.loopStarted.Store(true)
		started := make(chan error, 1)
		loopErr = started
		loopDone = make(chan struct{})
		go func() {
			defer close(loopDone)
			started <- a.superviseRoleLoop(runCtx)
		}()
	}
	select {
	case <-runCtx.Done():
		return nil
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) || runCtx.Err() != nil {
			return nil
		}
		if err == nil {
			return fmt.Errorf("%s health server stopped unexpectedly", a.role)
		}
		return fmt.Errorf("serve %s health: %w", a.role, err)
	case err := <-loopErr:
		if errors.Is(err, context.Canceled) || runCtx.Err() != nil {
			return nil
		}
		if err == nil {
			return fmt.Errorf("%s role loop stopped unexpectedly", a.role)
		}
		return fmt.Errorf("%s role loop: %w", a.role, err)
	}
}

func (a *App) superviseRoleLoop(ctx context.Context) error {
	for ctx.Err() == nil {
		a.loopReady.Store(true)
		_ = a.loop(ctx)
		a.loopReady.Store(false)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := waitPoll(ctx, a.config.PollInterval); err != nil {
			return err
		}
		if err := a.awaitReadiness(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		if a.streams != nil {
			groupCtx, cancel := context.WithTimeout(ctx, a.config.RPCDeadline)
			err := a.streams.EnsureGroup(groupCtx)
			cancel()
			if err != nil {
				continue
			}
		}
	}
	return ctx.Err()
}

// Shutdown cancels the role and waits for Run to stop within the supplied
// deadline. It does not close caller-owned database or Redis clients.
func (a *App) Shutdown(ctx context.Context) error {
	if a == nil || ctx == nil {
		return ErrInvalidConfig
	}
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	cancel, done := a.cancel, a.done
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c Config) validate(role Role) error {
	if c.Role != role || strings.TrimSpace(c.ListenAddr) == "" || c.StartupTimeout <= 0 || c.StartupTimeout > maxStartupTimeout ||
		c.RecoveryTimeout <= 0 || c.RecoveryTimeout > maxStartupTimeout || c.RPCDeadline <= 0 || c.RPCDeadline > maxRPCDeadline ||
		c.PollInterval <= 0 || c.PollInterval > maxPollInterval || c.ShutdownTimeout <= 0 || c.ShutdownTimeout > maxShutdownTimeout ||
		c.ReadinessTimeout <= 0 || c.ReadinessTimeout > maxReadinessWindow || c.ReadinessPollInterval <= 0 || c.ReadinessPollInterval > maxReadinessWindow ||
		c.MaxRequestBodyBytes <= 0 || c.MaxRequestBodyBytes > maxRequestBytes || c.DispatchBatch < 1 || c.DispatchBatch > maxDispatchBatch {
		return ErrInvalidConfig
	}
	if _, _, err := net.SplitHostPort(c.ListenAddr); err != nil {
		return fmt.Errorf("listen address: %w: %v", ErrInvalidConfig, err)
	}
	if role == RoleWorker && strings.TrimSpace(c.ConsumerName) == "" {
		return ErrInvalidConfig
	}
	return nil
}

func finiteWorkerConfig(consumer queue.ConsumerConfig, supervisorConfig supervisor.Config, appConfig Config) bool {
	if consumer.Block <= 0 || consumer.Block > maxRedisBlock || consumer.LeaseTTL <= 0 || consumer.LeaseTTL > maxWorkerLifetime ||
		consumer.LeaseTTL != supervisorConfig.LeaseTTL || consumer.ReclaimIdle < 0 || consumer.ReclaimIdle > maxWorkerLifetime ||
		consumer.OperationTimeout <= 0 || consumer.OperationTimeout > maxWorkerLifetime ||
		supervisorConfig.MaxExecution <= 0 || supervisorConfig.MaxExecution > maxWorkerLifetime ||
		supervisorConfig.StartTimeout <= 0 || supervisorConfig.StartTimeout > maxWorkerLifetime ||
		supervisorConfig.TermGrace <= 0 || supervisorConfig.TermGrace > maxWorkerLifetime ||
		supervisorConfig.KillWait <= 0 || supervisorConfig.KillWait > maxWorkerLifetime ||
		supervisorConfig.HeartbeatInterval <= 0 || supervisorConfig.HeartbeatInterval > maxWorkerLifetime {
		return false
	}
	minimumOperation := supervisorConfig.MaxExecution + supervisorConfig.StartTimeout + supervisorConfig.TermGrace + supervisorConfig.KillWait + appConfig.RPCDeadline
	minimumShutdown := consumer.Block + appConfig.RPCDeadline
	if termShutdown := supervisorConfig.StartTimeout + supervisorConfig.TermGrace + supervisorConfig.KillWait; termShutdown > minimumShutdown {
		minimumShutdown = termShutdown
	}
	return consumer.OperationTimeout >= minimumOperation && appConfig.ShutdownTimeout >= minimumShutdown
}

func (a *App) newHandler(business map[string]http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"status": "alive"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"status": "not_ready"})
			return
		}
		ready := a.Ready(r.Context())
		status := http.StatusOK
		label := "ready"
		if !ready.Ready {
			status = http.StatusServiceUnavailable
			label = "not_ready"
		}
		writeJSON(w, status, struct {
			Status       string   `json:"status"`
			Dependencies []string `json:"dependencies,omitempty"`
		}{Status: label, Dependencies: unavailableNames(ready.Dependencies)})
	})
	for path, handler := range business {
		businessHandler := handler
		mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ready := a.readiness(r.Context(), false)
			if !ready.Ready {
				writeJSON(w, http.StatusServiceUnavailable, struct {
					Status       string   `json:"status"`
					Dependencies []string `json:"dependencies"`
				}{Status: "not_ready", Dependencies: unavailableNames(ready.Dependencies)})
				return
			}
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, a.config.MaxRequestBodyBytes)
			}
			businessHandler.ServeHTTP(w, r)
		}))
	}
	return mux
}

func (a *App) awaitReadiness(ctx context.Context) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, a.config.ReadinessTimeout)
	defer cancel()
	ticker := time.NewTicker(a.config.ReadinessPollInterval)
	defer ticker.Stop()
	for {
		if a.readiness(deadlineCtx, false).Ready {
			return nil
		}
		select {
		case <-deadlineCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("role dependencies did not become ready: %w", ErrUnavailable)
		case <-ticker.C:
		}
	}
}

func (a *App) recoveryCheck(ctx context.Context) error {
	if !a.startupComplete.Load() {
		return errStartupIncomplete
	}
	return a.recovery.Ready(ctx)
}

func (a *App) runControl(ctx context.Context, pool *pgxpool.Pool, pushRouter *router.Router,
	dispatcher *dispatch.Dispatcher, reconciler *reconcile.Service) error {
	for ctx.Err() == nil {
		if err := a.waitForRecovery(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := waitPoll(ctx, a.config.PollInterval); err != nil {
				return err
			}
			continue
		}
		if err := a.routeInbox(ctx, pool, pushRouter); err != nil {
			return err
		}
		if err := a.waitForRecovery(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := waitPoll(ctx, a.config.PollInterval); err != nil {
				return err
			}
			continue
		}
		for dispatched := 0; dispatched < a.config.DispatchBatch; {
			if err := a.waitForRecovery(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				break
			}
			callCtx, cancel := context.WithTimeout(ctx, a.config.RPCDeadline)
			count, dispatchErr := dispatcher.DispatchDue(callCtx, 1)
			cancel()
			if dispatchErr != nil && count == 0 {
				return dispatchErr
			}
			if count == 0 {
				break
			}
			dispatched += count
		}
		if err := a.waitForRecovery(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := waitPoll(ctx, a.config.PollInterval); err != nil {
				return err
			}
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, a.config.RPCDeadline)
		_, reconcileErr := reconciler.RunOnce(callCtx)
		cancel()
		if reconcileErr != nil {
			return reconcileErr
		}
		if err := waitPoll(ctx, a.config.PollInterval); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (a *App) routeInbox(ctx context.Context, pool *pgxpool.Pool, pushRouter *router.Router) error {
	if err := a.waitForRecovery(ctx); err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, a.config.RPCDeadline)
	rows, err := pool.Query(callCtx, `SELECT delivery_id FROM webhook_deliveries
		WHERE COALESCE(disposition,'INBOX')='INBOX' ORDER BY received_at,delivery_id LIMIT $1`, a.config.DispatchBatch)
	if err != nil {
		cancel()
		return fmt.Errorf("read webhook inbox: %w", err)
	}
	var deliveryIDs []string
	for rows.Next() {
		var deliveryID string
		if err := rows.Scan(&deliveryID); err != nil {
			rows.Close()
			cancel()
			return fmt.Errorf("scan webhook inbox: %w", err)
		}
		deliveryIDs = append(deliveryIDs, deliveryID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		cancel()
		return fmt.Errorf("read webhook inbox rows: %w", err)
	}
	rows.Close()
	cancel()
	for _, deliveryID := range deliveryIDs {
		if err := a.waitForRecovery(ctx); err != nil {
			return err
		}
		routeCtx, routeCancel := context.WithTimeout(ctx, a.config.RPCDeadline)
		err := pushRouter.RouteDelivery(routeCtx, deliveryID)
		routeCancel()
		if err != nil {
			return fmt.Errorf("route webhook inbox delivery: %w", err)
		}
	}
	return nil
}

func (a *App) runWorker(ctx context.Context, streams *queue.Streams, consumer *queue.Consumer, config queue.ConsumerConfig) error {
	cursor := "0-0"
	for ctx.Err() == nil {
		if err := a.waitForRecovery(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := waitPoll(ctx, a.config.PollInterval); err != nil {
				return err
			}
			continue
		}
		pending, next, err := streams.Reclaim(ctx, a.config.ConsumerName, config.ReclaimIdle, cursor, config.BatchSize)
		if err != nil && ctx.Err() == nil {
			return err
		}
		if next != "" {
			cursor = next
		}
		if err := a.handleDeliveries(ctx, consumer, pending); err != nil {
			return err
		}
		if err := a.waitForRecovery(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := waitPoll(ctx, a.config.PollInterval); err != nil {
				return err
			}
			continue
		}
		fresh, err := streams.Read(ctx, a.config.ConsumerName, config.Block, config.BatchSize)
		if err != nil && ctx.Err() == nil {
			return err
		}
		if err := a.handleDeliveries(ctx, consumer, fresh); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (a *App) handleDeliveries(ctx context.Context, consumer *queue.Consumer, deliveries []queue.Delivery) error {
	for _, delivery := range deliveries {
		if err := a.waitForRecovery(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
		err := consumer.Handle(ctx, a.config.ConsumerName, delivery)
		if err != nil && !pendingDeliveryError(err) && ctx.Err() == nil {
			return err
		}
	}
	return nil
}

func (a *App) waitForRecovery(ctx context.Context) error {
	if ctx == nil {
		return ErrUnavailable
	}
	ready := a.readiness(ctx, false)
	if !ready.Ready {
		return ErrUnavailable
	}
	return nil
}

func waitPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func pendingDeliveryError(err error) bool {
	return errors.Is(err, ErrUnavailable) || errors.Is(err, queue.ErrLeaseActive) || errors.Is(err, queue.ErrMalformedDelivery) ||
		errors.Is(err, queue.ErrUnsupportedKind) || errors.Is(err, queue.ErrNotDisposed) ||
		errors.Is(err, queue.ErrInferenceAdmissionRequired) || errors.Is(err, queue.ErrReservationMissing) ||
		errors.Is(err, leases.ErrNotDue) || errors.Is(err, leases.ErrStale) ||
		errors.Is(err, budget.ErrEmergency) || errors.Is(err, budget.ErrTaskBudget) ||
		errors.Is(err, budget.ErrOrganizationBudget) || errors.Is(err, budget.ErrInvalidEnvelope) ||
		errors.Is(err, budget.ErrReservationConflict) || errors.Is(err, budget.ErrAdmission)
}

func makeCancelHandler(ack dispatch.Handler, timeout time.Duration) dispatch.Handler {
	return func(ctx context.Context, item dispatch.OutboxItem) error {
		if item.Kind != "CANCEL" || item.TaskID == "" || item.JobID == "" || len(item.Payload) == 0 || len(item.Payload) > maxCancelPayload {
			return fmt.Errorf("invalid cancellation outbox item: %w", ErrInvalidConfig)
		}
		var payload struct {
			TaskID string  `json:"task_id"`
			JobID  string  `json:"job_id"`
			RunID  *string `json:"run_id"`
		}
		decoder := json.NewDecoder(bytes.NewReader(item.Payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&payload); err != nil {
			return fmt.Errorf("decode cancellation target: %w", ErrInvalidConfig)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return fmt.Errorf("cancellation target has trailing data: %w", ErrInvalidConfig)
		}
		if payload.RunID == nil || payload.TaskID != item.TaskID || payload.JobID != item.JobID ||
			!validUUID(payload.TaskID) || !validUUID(payload.JobID) ||
			*payload.RunID != strings.TrimSpace(*payload.RunID) || (*payload.RunID != "" && !validUUID(*payload.RunID)) {
			return fmt.Errorf("cancellation target does not match outbox binding: %w", ErrInvalidConfig)
		}
		if *payload.RunID == "" {
			return nil
		}
		return invokeBounded(ctx, timeout, ack, item)
	}
}

func makeBoundedHandler(handler dispatch.Handler, timeout time.Duration) dispatch.Handler {
	return func(ctx context.Context, item dispatch.OutboxItem) error {
		if item.Kind != "NOTIFY" {
			return fmt.Errorf("invalid notification outbox item: %w", ErrInvalidConfig)
		}
		return invokeBounded(ctx, timeout, handler, item)
	}
}

func invokeBounded(ctx context.Context, timeout time.Duration, handler dispatch.Handler, item dispatch.OutboxItem) error {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := handler(callCtx, item)
	if err != nil {
		return err
	}
	return callCtx.Err()
}

func pgCheck(pool *pgxpool.Pool) func(context.Context) error {
	return func(ctx context.Context) error { return pool.Ping(ctx) }
}

func redisCheck(client *redis.Client) func(context.Context) error {
	return func(ctx context.Context) error { return client.Ping(ctx).Err() }
}

func setDependency(readiness *Readiness, name string, ready bool, code string) {
	for i := range readiness.Dependencies {
		if readiness.Dependencies[i].Name == name {
			readiness.Dependencies[i].Ready = ready
			readiness.Dependencies[i].Code = code
			return
		}
	}
	readiness.Dependencies = append(readiness.Dependencies, DependencyStatus{Name: name, Ready: ready, Code: code})
}

func unavailableNames(dependencies []DependencyStatus) []string {
	names := make([]string, 0, len(dependencies))
	for _, dependency := range dependencies {
		if !dependency.Ready {
			names = append(names, dependency.Name)
		}
	}
	sort.Strings(names)
	return names
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"status":"not_ready"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if char < '0' || char > '9' && char < 'a' || char > 'f' {
			return false
		}
	}
	return true
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	return isNilValue(value)
}

func isNilValue(value any) bool {
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return ref.IsNil()
	default:
		return false
	}
}
