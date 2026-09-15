package steps

import (
	"context"
	"fmt"
	"os"

	"github.com/elastic/orca/elastic"
	"github.com/elastic/orca/harness"
)

func NewEsAggFactory(esCfg harness.ESConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			es, err := buildESClient(state, esCfg)
			if err != nil {
				return fmt.Errorf("es-agg: %w", err)
			}
			cfg := step.EsAgg
			index, err := state.Resolve(cfg.Index)
			if err != nil {
				return err
			}
			since, err := resolveTime(state, cfg.Since)
			if err != nil {
				return fmt.Errorf("since: %w", err)
			}
			until, err := resolveTime(state, cfg.Until)
			if err != nil {
				return fmt.Errorf("until: %w", err)
			}
			filter, err := resolveFilter(state, cfg.Filter)
			if err != nil {
				return err
			}

			result, hitCount, err := elastic.RunAgg(ctx, es, index, since, until, filter, cfg.Metrics)
			if err != nil {
				return fmt.Errorf("es-agg: %w", err)
			}
			if hitCount == 0 {
				fmt.Fprintf(os.Stderr,
					"[es-agg] warning: 0 documents matched (index=%s since=%s until=%s) — check that Fleet monitoring is enabled, and verify filter values and field names in Kibana Discover\n",
					index, cfg.Since, cfg.Until,
				)
			}

			if step.ID == "" {
				return nil
			}
			for k, v := range result {
				state.SetStepOutput(step.ID, k, v)
			}
			return nil
		}), nil
	}
}
