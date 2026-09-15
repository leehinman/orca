package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// State is the shared value store for a benchmark run.
// Keys match the template reference paths: "scratch",
// "phase.<name>.start", "steps.<id>.<key>", etc.
type State struct {
	m map[string]any
}

// NewState returns an empty State.
func NewState() *State {
	return &State{m: make(map[string]any)}
}

// Set writes a raw key/value pair into state, equivalent to what a step would
// store via SetStepOutput.
func (s *State) Set(key, value string) {
	s.m[key] = value
}

// SetScratch records the per-run temp directory path.
func (s *State) SetScratch(path string) {
	s.m["scratch"] = path
}

// SetResults records the results directory path for the current bench iteration.
func (s *State) SetResults(path string) {
	s.m["results"] = path
}

// SetRunDir records the run directory path.
func (s *State) SetRunDir(path string) {
	s.m["run"] = path
}

// SetPhaseStart records the RFC3339 start time for a phase.
func (s *State) SetPhaseStart(phase string, t time.Time) {
	s.m["phase."+phase+".start"] = t.UTC().Format(time.RFC3339)
}

// SetPhaseEnd records the RFC3339 end time for a phase.
func (s *State) SetPhaseEnd(phase string, t time.Time) {
	s.m["phase."+phase+".end"] = t.UTC().Format(time.RFC3339)
}

// SetPhaseDuration records the human-readable duration for a phase.
func (s *State) SetPhaseDuration(phase string, d time.Duration) {
	s.m["phase."+phase+".duration"] = d.Round(time.Millisecond).String()
}

// SetStepOutput stores a step output value under steps.<id>.<key>.
func (s *State) SetStepOutput(stepID, key string, val any) {
	s.m["steps."+stepID+"."+key] = val
}

// ClearStepOutputs removes every output stored under steps.<id>. for the given
// step IDs so a repeated bench cannot report a previous iteration's results.
func (s *State) ClearStepOutputs(ids []string) {
	for _, id := range ids {
		prefix := "steps." + id + "."
		for k := range s.m {
			if strings.HasPrefix(k, prefix) {
				delete(s.m, k)
			}
		}
	}
}

// Save serializes the state map to a JSON file at path.
func (s *State) Save(path string) error {
	data, err := json.Marshal(s.m)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// LoadState deserializes state from a JSON file previously written by Save.
func LoadState(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse state file: %w", err)
	}
	return &State{m: m}, nil
}

// Resolve expands all ${...} references in str and returns the result.
// Values are resolved from the state map first, then from environment variables
// (for keys that are not step or phase references). Returns an error if any
// reference cannot be resolved.
func (s *State) Resolve(str string) (string, error) {
	var resolveErr error
	result := refRE.ReplaceAllStringFunc(str, func(match string) string {
		if resolveErr != nil {
			return ""
		}
		key, def, hasDefault := splitRefDefault(match[2 : len(match)-1])
		val, ok := s.lookup(key)
		if !ok {
			if hasDefault {
				return def
			}
			resolveErr = fmt.Errorf("unresolved reference ${%s}", key)
			return ""
		}
		return fmt.Sprintf("%v", val)
	})
	if resolveErr != nil {
		return "", resolveErr
	}
	return result, nil
}

func (s *State) lookup(key string) (any, bool) {
	if v, ok := s.m[key]; ok {
		return v, true
	}
	// Env var fallback only for non-structural keys.
	if strings.HasPrefix(key, "steps.") || strings.HasPrefix(key, "phase.") || key == "scratch" || key == "results" || key == "run" {
		return nil, false
	}
	if v := os.Getenv(key); v != "" {
		return v, true
	}
	return nil, false
}

// splitRefDefault splits a ${NAME:-default} reference body into its name and
// default value. The default is used when NAME is not set.
func splitRefDefault(ref string) (name, def string, ok bool) {
	if i := strings.Index(ref, ":-"); i >= 0 {
		return ref[:i], ref[i+2:], true
	}
	return ref, "", false
}
