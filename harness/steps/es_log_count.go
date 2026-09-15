package steps

import (
	"context"
	"fmt"
	"time"

	"github.com/elastic/orca/elastic"
	"github.com/elastic/orca/harness"
)

const defaultPollInterval = 5 * time.Second

func NewEsDocCountFactory(esCfg harness.ESConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			es, err := buildESClient(state, esCfg)
			if err != nil {
				return fmt.Errorf("es-doc-count: %w", err)
			}
			cfg := step.EsDocCount
			index, err := state.Resolve(cfg.Index)
			if err != nil {
				return err
			}
			since, err := resolveTime(state, cfg.Since)
			if err != nil {
				return fmt.Errorf("since: %w", err)
			}
			filter, err := resolveFilter(state, cfg.Filter)
			if err != nil {
				return err
			}
			if cfg.GroupBy != "" {
				counts, err := elastic.PollGroupedLogCount(ctx, es, index, since, filter, cfg.GroupBy, int(cfg.Expected), cfg.ExpectedGroups, cfg.Timeout.Duration, defaultPollInterval)
				if err != nil {
					return fmt.Errorf("poll doc count: %w", err)
				}
				if step.ID != "" {
					for group, n := range counts {
						state.SetStepOutput(step.ID, group, n)
					}
					state.SetStepOutput(step.ID, "confirmedAt", time.Now().UTC().Format(time.RFC3339))
				}
				return nil
			}
			actual, err := elastic.PollLogCount(ctx, es, index, since, filter, int(cfg.Expected), cfg.Timeout.Duration, defaultPollInterval)
			if err != nil {
				return fmt.Errorf("poll doc count: %w", err)
			}
			if step.ID != "" {
				state.SetStepOutput(step.ID, "actualCount", actual)
				state.SetStepOutput(step.ID, "confirmedAt", time.Now().UTC().Format(time.RFC3339))
			}
			return nil
		}), nil
	}
}
