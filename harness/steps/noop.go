package steps

import (
	"context"
	"fmt"

	"github.com/elastic/orca/harness"
)

func NewNoopFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(_ context.Context, state *harness.State) error {
			for k, v := range step.Noop.State {
				resolved, err := state.Resolve(v)
				if err != nil {
					return fmt.Errorf("noop state %q: %w", k, err)
				}
				state.SetStepOutput(step.ID, k, resolved)
			}
			return nil
		}), nil
	}
}
