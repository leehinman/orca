package steps

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/elastic/orca/harness"
)

func NewWriteIniFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.WriteIni

			outPath, err := state.Resolve(cfg.Output)
			if err != nil {
				return err
			}
			outPath, err = filepath.Abs(outPath)
			if err != nil {
				return fmt.Errorf("resolve output path: %w", err)
			}

			if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
				return fmt.Errorf("create output directory: %w", err)
			}

			var buf strings.Builder
			for i, sec := range cfg.Sections {
				if i > 0 {
					buf.WriteByte('\n')
				}
				fmt.Fprintf(&buf, "[%s]\n", sec.Name)
				for _, entry := range sec.Entries {
					resolved, err := state.Resolve(entry)
					if err != nil {
						return fmt.Errorf("section %q entry %q: %w", sec.Name, entry, err)
					}
					fmt.Fprintln(&buf, resolved)
				}
			}

			if err := os.WriteFile(outPath, []byte(buf.String()), 0o644); err != nil {
				return fmt.Errorf("write ini file: %w", err)
			}

			fmt.Fprintf(os.Stderr, "[write-ini] wrote %d section(s) to %s\n", len(cfg.Sections), outPath)
			return nil
		}), nil
	}
}
