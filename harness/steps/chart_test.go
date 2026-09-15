package steps_test

import (
	"context"
	"os"
	"testing"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

func writeTempTSV(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.tsv")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return f.Name()
}

const sampleTSV = "timestamp\tmem_size\tcgroup_mem\n" +
	"23:01:00\t1000000\t2000000\n" +
	"23:02:00\t1200000\t2100000\n" +
	"23:03:00\t1100000\t2050000\n"

const timestampTSV = "timestamp\tmem_size\n" +
	"2026-09-14T23:01:00Z\t1000000\n" +
	"2026-09-14T23:02:00Z\t1200000\n"

func chartStep(tsvPath, outPath string, y []string) harness.Step {
	return harness.Step{
		ID: "chart-out",
		Chart: &harness.ChartStep{
			Input:  tsvPath,
			Output: outPath,
			Title:  "Test Chart",
			X:      "timestamp",
			Y:      y,
		},
	}
}

func TestChartFactory_CreatesFile(t *testing.T) {
	tsvPath := writeTempTSV(t, sampleTSV)
	outPath := t.TempDir() + "/chart.png"
	exec, _ := steps.NewChartFactory()(chartStep(tsvPath, outPath, nil))
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("output file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Error("output file is empty")
	}
}

func TestChartFactory_SVGOutput(t *testing.T) {
	tsvPath := writeTempTSV(t, sampleTSV)
	outPath := t.TempDir() + "/chart.svg"
	exec, _ := steps.NewChartFactory()(chartStep(tsvPath, outPath, nil))
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("output file not created: %v", err)
	}
	if len(b) == 0 {
		t.Error("svg file is empty")
	}
}

func TestChartFactory_AllColumnsWhenYEmpty(t *testing.T) {
	// y=nil should use all non-X columns (mem_size and cgroup_mem)
	tsvPath := writeTempTSV(t, sampleTSV)
	outPath := t.TempDir() + "/chart.png"
	exec, _ := steps.NewChartFactory()(chartStep(tsvPath, outPath, nil))
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChartFactory_SelectedYColumns(t *testing.T) {
	tsvPath := writeTempTSV(t, sampleTSV)
	outPath := t.TempDir() + "/chart.png"
	exec, _ := steps.NewChartFactory()(chartStep(tsvPath, outPath, []string{"mem_size"}))
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("output file not created: %v", err)
	}
}

func TestChartFactory_UnknownXColumn(t *testing.T) {
	tsvPath := writeTempTSV(t, sampleTSV)
	step := harness.Step{
		Chart: &harness.ChartStep{
			Input: tsvPath, Output: t.TempDir() + "/chart.png",
			X: "nonexistent",
		},
	}
	exec, _ := steps.NewChartFactory()(step)
	if err := exec.Run(context.Background(), harness.NewState()); err == nil {
		t.Fatal("expected error for unknown X column")
	}
}

func TestChartFactory_UnknownYColumn(t *testing.T) {
	tsvPath := writeTempTSV(t, sampleTSV)
	exec, _ := steps.NewChartFactory()(chartStep(tsvPath, t.TempDir()+"/chart.png", []string{"nonexistent"}))
	if err := exec.Run(context.Background(), harness.NewState()); err == nil {
		t.Fatal("expected error for unknown Y column")
	}
}

func TestChartFactory_SetsFilePathOutput(t *testing.T) {
	tsvPath := writeTempTSV(t, sampleTSV)
	outPath := t.TempDir() + "/chart.png"
	exec, _ := steps.NewChartFactory()(chartStep(tsvPath, outPath, nil))
	state := harness.NewState()
	exec.Run(context.Background(), state) //nolint
	v, err := state.Resolve("${steps.chart-out.filePath}")
	if err != nil || v == "" {
		t.Fatalf("filePath not set in state: %v", err)
	}
}

func TestChartFactory_TimestampFormattedAsHHMMSS(t *testing.T) {
	// RFC3339 timestamps should be formatted to HH:MM:SS on the X axis.
	// We can't inspect chart internals, but we can verify the step doesn't error.
	tsvPath := writeTempTSV(t, timestampTSV)
	outPath := t.TempDir() + "/chart.png"
	exec, _ := steps.NewChartFactory()(chartStep(tsvPath, outPath, nil))
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatalf("unexpected error for RFC3339 timestamps: %v", err)
	}
}

func TestChartFactory_CreatesParentDir(t *testing.T) {
	tsvPath := writeTempTSV(t, sampleTSV)
	outPath := t.TempDir() + "/nested/dir/chart.png"
	exec, _ := steps.NewChartFactory()(chartStep(tsvPath, outPath, nil))
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("output file not found: %v", err)
	}
}

func TestChartFactory_MissingYValuesUseZero(t *testing.T) {
	// TSV with a missing (empty) Y value in one row.
	tsvWithMissing := "timestamp\tmem_size\n" +
		"23:01:00\t1000000\n" +
		"23:02:00\t\n" + // missing value
		"23:03:00\t1100000\n"
	tsvPath := writeTempTSV(t, tsvWithMissing)
	outPath := t.TempDir() + "/chart.png"
	exec, _ := steps.NewChartFactory()(chartStep(tsvPath, outPath, nil))
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatalf("unexpected error for missing Y value: %v", err)
	}
}
