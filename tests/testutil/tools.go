package testutil

// Tool versions and external scratch layout used by the APRL Go workflow.
const (
	// GoToolchainVersion is the required Go toolchain.
	GoToolchainVersion = "go1.27.1"
	// GoImportsVersion is the pinned goimports module version.
	GoImportsVersion = "golang.org/x/tools v0.50.0"
	// GolangCILintVersion is the pinned golangci-lint release.
	GolangCILintVersion = "golangci-lint v2.14.0"
	// ScratchRoot is the external volume root supplied through APRL_SCRATCH.
	ScratchRoot = "${APRL_SCRATCH}"
	// GoCachePath is the external Go build cache path.
	GoCachePath = "${APRL_SCRATCH}/cache"
	// GoModuleCachePath is the external Go module cache path.
	GoModuleCachePath = "${APRL_SCRATCH}/modcache"
	// GoTemporaryPath is the external Go temporary directory.
	GoTemporaryPath = "${APRL_SCRATCH}/tmp"
	// ToolsPath is the external directory for pinned tools.
	ToolsPath = "${APRL_SCRATCH}/tools"
)
