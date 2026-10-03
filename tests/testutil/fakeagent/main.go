// fakeagent is a bounded child-process fixture used only by supervisor tests.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ajent-social/APRL/internal/contracts"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--descendant-holder" {
		// This fixture exits on its own so the test can verify that a live
		// descendant after leader exit remains charged until the host observes
		// the owned group drain. It is never signaled by a stale numeric PGID.
		time.Sleep(2 * time.Second)
		return
	}
	ignoreTERM := false
	holdAfterActivation := false
	spawnDescendant := false
	for _, argument := range os.Args[1:] {
		switch argument {
		case "--ignore-term":
			ignoreTERM = true
		case "--hold":
			holdAfterActivation = true
		case "--spawn-descendant":
			spawnDescendant = true
		}
	}
	if hasHostSecretEnvironment() {
		fmt.Fprintln(os.Stderr, "host secret environment was inherited")
		os.Exit(70)
	}
	reader := bufio.NewReader(os.Stdin)
	decoder := json.NewDecoder(reader)
	var result contracts.Result
	if err := decoder.Decode(&result); err != nil {
		fmt.Fprintln(os.Stderr, "read activation input:", err)
		os.Exit(71)
	}
	if ignoreTERM {
		caught := make(chan os.Signal, 1)
		signal.Notify(caught, syscall.SIGTERM)
		defer signal.Stop(caught)
		fmt.Fprintln(os.Stderr, "__APRL_FAKEAGENT_READY__")
		<-caught
		select {}
	}
	if holdAfterActivation {
		var release struct {
			Release bool `json:"release"`
		}
		if err := decoder.Decode(&release); err != nil || !release.Release {
			fmt.Fprintln(os.Stderr, "read release input:", err)
			os.Exit(72)
		}
	}
	if spawnDescendant {
		binary, err := os.Executable()
		if err != nil {
			fmt.Fprintln(os.Stderr, "resolve fixture executable:", err)
			os.Exit(74)
		}
		child := exec.Command(binary, "--descendant-holder")
		child.Env = os.Environ()
		child.Stdin = strings.NewReader("")
		child.Stdout = io.Discard
		child.Stderr = io.Discard
		if err := child.Start(); err != nil {
			fmt.Fprintln(os.Stderr, "start descendant fixture:", err)
			os.Exit(75)
		}
	}
	result.Status = "succeeded"
	result.Summary = "fixture completed"
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "write result:", err)
		os.Exit(73)
	}
}

func hasHostSecretEnvironment() bool {
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(name)
		for _, marker := range []string{"SECRET", "TOKEN", "CREDENTIAL", "APP_PRIVATE", "_KEY"} {
			if strings.Contains(upper, marker) {
				return true
			}
		}
	}
	return false
}
