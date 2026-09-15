package steps

import (
	"fmt"
	"os"
	"strconv"

	gocharts "github.com/go-analyze/charts"
)

// renderLineChart renders a line chart from a TSV file.
func renderLineChart(outPath, title, tsvPath, xCol string, yCols []string) error {
	header, rows, err := readTSV(tsvPath)
	if err != nil {
		return fmt.Errorf("read tsv %s: %w", tsvPath, err)
	}
	if len(rows) == 0 {
		fmt.Fprintf(os.Stderr, "[metrics] warning: no data rows for chart %s\n", outPath)
		return nil
	}

	colIdx := makeColumnIndex(header)
	xIdx, ok := colIdx[xCol]
	if !ok {
		return fmt.Errorf("x column %q not found in %v", xCol, header)
	}
	yIndices := make([]int, 0, len(yCols))
	for _, name := range yCols {
		idx, ok := colIdx[name]
		if !ok {
			return fmt.Errorf("y column %q not found in %v", name, header)
		}
		yIndices = append(yIndices, idx)
	}

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

	opt := gocharts.NewLineChartOptionWithData(ySeries)
	opt.Title = gocharts.TitleOption{Text: title}
	opt.XAxis.Labels = xLabels
	opt.Legend.SeriesNames = yCols

	p := gocharts.NewPainter(gocharts.PainterOptions{
		Width:        1000,
		Height:       400,
		OutputFormat: chartOutputFormat(outPath),
		Font:         gocharts.GetDefaultFont(),
	})
	if err := p.LineChart(opt); err != nil {
		return fmt.Errorf("render chart: %w", err)
	}
	buf, err := p.Bytes()
	if err != nil {
		return fmt.Errorf("encode chart: %w", err)
	}
	if err := os.WriteFile(outPath, buf, 0o644); err != nil {
		return fmt.Errorf("write chart: %w", err)
	}
	fmt.Fprintf(os.Stderr, "[metrics] wrote chart %s (%d points)\n", outPath, len(xLabels))
	return nil
}
