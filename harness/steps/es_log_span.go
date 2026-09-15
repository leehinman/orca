package steps

import (
	"context"
	"fmt"
	"time"

	"github.com/elastic/orca/elastic"
	"github.com/elastic/orca/harness"
)

func NewEsLogSpanFactory(esCfg harness.ESConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			es, err := buildESClient(state, esCfg)
			if err != nil {
				return fmt.Errorf("es-log-span: %w", err)
			}
			cfg := step.EsLogSpan
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
			spans, err := elastic.PollGroupedLogSpan(
				ctx, es, index, since, filter,
				cfg.GroupBy, cfg.MinDuration.Duration,
				int(cfg.ExpectedGroups), cfg.Timeout.Duration, defaultPollInterval,
			)
			if err != nil {
				return fmt.Errorf("poll log span: %w", err)
			}
			if step.ID != "" {
				for group, d := range spans {
					state.SetStepOutput(step.ID, group, d.String())
				}
				state.SetStepOutput(step.ID, "confirmedAt", time.Now().UTC().Format(time.RFC3339))
			}
			return nil
		}), nil
	}
}
