package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	currentRunFile = ".orca/current"
	stateFileName  = "orca-state.json"
)

// CurrentRun returns the run directory recorded by the most recent up or run
// command in the working directory.
func CurrentRun() (string, error) {
	data, err := os.ReadFile(currentRunFile)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("no current run: pass --run or start one with orca up")
		}
		return "", fmt.Errorf("read current run: %w", err)
	}
	dir := strings.TrimSpace(string(data))
	if dir == "" {
		return "", fmt.Errorf("no current run: pass --run or start one with orca up")
	}
	return dir, nil
}

// LoadRun loads the saved state for the run directory dir.
func LoadRun(dir string) (*State, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve run dir: %w", err)
	}
	state, err := LoadState(filepath.Join(dir, stateFileName))
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}
	if _, err := state.Resolve("${run}"); err != nil {
		state.SetRunDir(dir)
	}
	return state, nil
}

func setCurrentRun(dir string) error {
	if err := os.MkdirAll(filepath.Dir(currentRunFile), 0o755); err != nil {
		return err
	}
	return os.WriteFile(currentRunFile, []byte(dir+"\n"), 0o644)
}

func clearCurrentRun(dir string) {
	cur, err := CurrentRun()
	if err != nil || cur != dir {
		return
	}
	os.Remove(currentRunFile) //nolint:errcheck
}
