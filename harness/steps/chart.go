package steps

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/orca/harness"
	"github.com/go-analyze/charts"
)

func NewChartFactory() harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			cfg := step.Chart

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
			title, _ := state.Resolve(cfg.Title)

			header, rows, err := readTSV(inputPath)
			if err != nil {
				return fmt.Errorf("read tsv %s: %w", inputPath, err)
			}
			if len(rows) == 0 {
				return fmt.Errorf("tsv %s has no data rows", inputPath)
			}

			colIdx := makeColumnIndex(header)

			xIdx, ok := colIdx[cfg.X]
			if !ok {
				return fmt.Errorf("x column %q not found in header %v", cfg.X, header)
			}

			yNames := cfg.Y
			if len(yNames) == 0 {
				for _, name := range header {
					if name != cfg.X {
						yNames = append(yNames, name)
					}
				}
			}
			yIndices := make([]int, 0, len(yNames))
			for _, name := range yNames {
				idx, ok := colIdx[name]
				if !ok {
					return fmt.Errorf("y column %q not found in header %v", name, header)
				}
				yIndices = append(yIndices, idx)
			}

			// Collect aligned X labels and Y series values.
			xLabels := make([]string, 0, len(rows))
			ySeries := make([][]float64, len(yIndices))
			for i := range ySeries {
				ySeries[i] = make([]float64, 0, len(rows))
			}
			for _, row := range rows {
				if xIdx >= len(row) || row[xIdx] == "" {
					continue
				}
				xLabels = append(xLabels, formatChartLabel(row[xIdx]))
				for i, yIdx := range yIndices {
					var v float64
					if yIdx < len(row) && row[yIdx] != "" {
						v, _ = strconv.ParseFloat(row[yIdx], 64)
					}
					ySeries[i] = append(ySeries[i], v)
				}
			}

			w, h := cfg.Width, cfg.Height
			if w == 0 {
				w = 800
			}
			if h == 0 {
				h = 400
			}

			opt := charts.NewLineChartOptionWithData(ySeries)
			opt.Title = charts.TitleOption{Text: title}
			opt.XAxis.Labels = xLabels
			opt.Legend.SeriesNames = yNames

			p := charts.NewPainter(charts.PainterOptions{
				Width:        w,
				Height:       h,
				OutputFormat: chartOutputFormat(outPath),
				Font:         charts.GetDefaultFont(),
			})
			if err := p.LineChart(opt); err != nil {
				return fmt.Errorf("render chart: %w", err)
			}
			buf, err := p.Bytes()
			if err != nil {
				return fmt.Errorf("encode chart: %w", err)
			}

			if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
				return fmt.Errorf("create output dir: %w", err)
			}
			if err := os.WriteFile(outPath, buf, 0o644); err != nil {
				return fmt.Errorf("write chart: %w", err)
			}

			fmt.Fprintf(os.Stderr, "[chart] wrote %s (%d data points)\n", outPath, len(xLabels))

			if step.ID != "" {
				state.SetStepOutput(step.ID, "filePath", outPath)
			}
			return nil
		}), nil
	}
}

// readTSV parses a tab-separated file into a header slice and data rows.
func readTSV(path string) (header []string, rows [][]string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close() //nolint:errcheck
	r := bufio.NewReader(f)
	first := true
	for {
		line, err := r.ReadString('\n')
		if line == "" && err != nil {
			if err == io.EOF {
				return header, rows, nil
			}
			return nil, nil, err
		}
		cols := strings.Split(strings.TrimRight(line, "\r\n"), "\t")
		if first {
			header = cols
			first = false
		} else {
			rows = append(rows, cols)
		}
		if err == io.EOF {
			return header, rows, nil
		}
	}
}

func makeColumnIndex(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, name := range header {
		m[name] = i
	}
	return m
}

// formatChartLabel converts an RFC3339 timestamp to HH:MM:SS for axis
// readability. Non-timestamp values are returned unchanged.
func formatChartLabel(s string) string {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().Format("15:04:05")
		}
	}
	return s
}

func chartOutputFormat(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".svg":
		return charts.ChartOutputSVG
	case ".jpg", ".jpeg":
		return charts.ChartOutputJPG
	default:
		return charts.ChartOutputPNG
	}
}
