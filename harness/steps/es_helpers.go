package steps

import (
	"fmt"
	"time"

	"github.com/elastic/orca/harness"
)

func resolveFilter(state *harness.State, filter map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(filter))
	for k, v := range filter {
		rv, err := state.Resolve(v)
		if err != nil {
			return nil, fmt.Errorf("filter %q: %w", k, err)
		}
		out[k] = rv
	}
	return out, nil
}

func resolveTime(state *harness.State, s string) (time.Time, error) {
	resolved, err := state.Resolve(s)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339, resolved)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time %q: %w", resolved, err)
	}
	return t, nil
}
