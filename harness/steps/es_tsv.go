package steps

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/elastic/orca/elastic"
	"github.com/elastic/orca/harness"
)

func NewEsTsvFactory(esCfg harness.ESConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			es, err := buildESClient(state, esCfg)
			if err != nil {
				return fmt.Errorf("es-tsv: %w", err)
			}
			cfg := step.EsTsv
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
			outPath, err := state.Resolve(cfg.Output)
			if err != nil {
				return err
			}
			outPath, err = filepath.Abs(outPath)
			if err != nil {
				return fmt.Errorf("resolve output path: %w", err)
			}
			filter, err := resolveFilter(state, cfg.Filter)
			if err != nil {
				return err
			}

			docs, err := elastic.FetchDocs(ctx, es, index, since, until, filter, cfg.Size)
			if err != nil {
				return fmt.Errorf("es-tsv: %w", err)
			}
			if len(docs) == 0 {
				fmt.Fprintf(os.Stderr,
					"[es-tsv] warning: 0 documents matched (index=%s since=%s until=%s) — check filter values and field names\n",
					index, cfg.Since, cfg.Until,
				)
			}

			if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
				return fmt.Errorf("create output directory: %w", err)
			}
			f, err := os.Create(outPath)
			if err != nil {
				return fmt.Errorf("create tsv file: %w", err)
			}
			defer f.Close() //nolint:errcheck

			names := make([]string, len(cfg.Metrics))
			for i, m := range cfg.Metrics {
				names[i] = m.Name
			}
			fmt.Fprintln(f, strings.Join(names, "\t"))

			written := 0
			for _, doc := range docs {
				skip := false
				for _, m := range cfg.Metrics {
					if m.Required {
						if _, ok := elastic.GetFieldValue(doc, m.Field); !ok {
							skip = true
							break
						}
					}
				}
				if skip {
					continue
				}

				var row strings.Builder
				for i, m := range cfg.Metrics {
					if i > 0 {
						row.WriteByte('\t')
					}
					if val, ok := elastic.GetFieldValue(doc, m.Field); ok {
						row.WriteString(formatValue(val))
					}
				}
				fmt.Fprintln(f, row.String())
				written++
			}

			fmt.Fprintf(os.Stderr, "[es-tsv] wrote %d rows to %s\n", written, outPath)

			if step.ID != "" {
				state.SetStepOutput(step.ID, "filePath", outPath)
				state.SetStepOutput(step.ID, "rowCount", int64(written))
			}
			return nil
		}), nil
	}
}

// formatValue converts a JSON-decoded value to its TSV string representation.
// JSON numbers (always float64) are formatted as integers when they have no
// fractional part, to avoid "1.88558e+09" for byte counts.
func formatValue(v interface{}) string {
	switch val := v.(type) {
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case string:
		return val
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}
