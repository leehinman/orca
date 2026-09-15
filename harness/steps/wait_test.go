package steps

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/elastic/orca/harness"
)

func TestWaitFactory_RejectsNonPositiveDuration(t *testing.T) {
	if _, err := NewWaitFactory()(harness.Step{Wait: &harness.WaitStep{}}); err == nil {
		t.Fatal("expected error for zero duration")
	}
}

func TestWaitFor_CompletesAndLogsProgress(t *testing.T) {
	var buf bytes.Buffer
	start := time.Now()
	if err := waitFor(context.Background(), 250*time.Millisecond, 100*time.Millisecond, &buf); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 250*time.Millisecond {
		t.Errorf("returned early after %s", el)
	}
	out := buf.String()
	if !strings.Contains(out, "waiting 250ms") || strings.Count(out, "remaining") < 2 {
		t.Errorf("unexpected log output:\n%s", out)
	}
}

func TestWaitFor_Cancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	start := time.Now()
	err := waitFor(ctx, time.Hour, time.Hour, &bytes.Buffer{})
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("expected prompt cancellation error, got %v after %s", err, time.Since(start))
	}
}

func TestWaitFactory_RunsStep(t *testing.T) {
	exec, err := NewWaitFactory()(harness.Step{Wait: &harness.WaitStep{Duration: harness.Duration{Duration: 20 * time.Millisecond}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Run(context.Background(), harness.NewState()); err != nil {
		t.Fatal(err)
	}
}
