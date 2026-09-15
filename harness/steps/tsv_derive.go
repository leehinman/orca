package steps

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/orca/harness"
)

func NewTsvDeriveFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.TsvDerive

			inputPath, err := state.Resolve(cfg.Input)
			if err != nil {
				return err
			}
			outPath, err := state.Resolve(cfg.Output)
			if err != nil {
				return err
			}
			outPath, err = filepath.Abs(outPath)
			if err != nil {
				return fmt.Errorf("resolve output path: %w", err)
			}

			header, rows, err := readTSV(inputPath)
			if err != nil {
				return fmt.Errorf("read tsv %s: %w", inputPath, err)
			}

			colIdx := makeColumnIndex(header)

			timeIdx, ok := colIdx[cfg.Time]
			if !ok {
				return fmt.Errorf("time column %q not found in %v", cfg.Time, header)
			}

			type deriveSrc struct {
				fromIdx     int
				divisorIdx  int // -1 if no divisor column
				scale       float64
			}
			srcs := make([]deriveSrc, len(cfg.Columns))
			for i, col := range cfg.Columns {
				idx, ok := colIdx[col.From]
				if !ok {
					return fmt.Errorf("source column %q not found in %v", col.From, header)
				}
				scale := col.Scale
				if scale == 0 {
					scale = 1.0
				}
				divisorIdx := -1
				if col.Divisor != "" {
					di, ok := colIdx[col.Divisor]
					if !ok {
						return fmt.Errorf("divisor column %q not found in %v", col.Divisor, header)
					}
					divisorIdx = di
				}
				srcs[i] = deriveSrc{fromIdx: idx, divisorIdx: divisorIdx, scale: scale}
			}

			// Output header: original columns + new derived column names.
			newNames := make([]string, len(cfg.Columns))
			for i, col := range cfg.Columns {
				newNames[i] = col.Name
			}
			outHeader := append(append([]string{}, header...), newNames...)

			if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
				return fmt.Errorf("create output directory: %w", err)
			}
			f, err := os.Create(outPath)
			if err != nil {
				return fmt.Errorf("create tsv file: %w", err)
			}
			defer f.Close()

			fmt.Fprintln(f, strings.Join(outHeader, "\t"))

			var prevTime time.Time
			prevVals := make([]float64, len(cfg.Columns))
			hasPrev := false
			written := 0

			for _, row := range rows {
				var t time.Time
				if timeIdx < len(row) && row[timeIdx] != "" {
					for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
						if pt, err := time.Parse(layout, row[timeIdx]); err == nil {
							t = pt
							break
						}
					}
				}

				var line strings.Builder
				line.WriteString(strings.Join(row, "\t"))

				for i, src := range srcs {
					line.WriteByte('\t')
					if hasPrev && !t.IsZero() && !prevTime.IsZero() {
						var curVal float64
						if src.fromIdx < len(row) && row[src.fromIdx] != "" {
							curVal, _ = strconv.ParseFloat(row[src.fromIdx], 64)
						}
						deltaMs := t.Sub(prevTime).Seconds() * 1000
						if deltaMs > 0 {
							derived := (curVal - prevVals[i]) / deltaMs * src.scale
							if src.divisorIdx >= 0 && src.divisorIdx < len(row) && row[src.divisorIdx] != "" {
								if divisor, err := strconv.ParseFloat(row[src.divisorIdx], 64); err == nil && divisor != 0 {
									derived /= divisor
								}
							}
							line.WriteString(strconv.FormatFloat(derived, 'f', 4, 64))
						}
					}
				}

				if !t.IsZero() {
					prevTime = t
					for i, src := range srcs {
						if src.fromIdx < len(row) && row[src.fromIdx] != "" {
							prevVals[i], _ = strconv.ParseFloat(row[src.fromIdx], 64)
						}
					}
					hasPrev = true
				}

				fmt.Fprintln(f, line.String())
				written++
			}

			fmt.Fprintf(os.Stderr, "[tsv-derive] wrote %d rows to %s\n", written, outPath)

			if step.ID != "" {
				state.SetStepOutput(step.ID, "filePath", outPath)
			}
			return nil
		}), nil
	}
}
