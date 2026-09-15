package elastic

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitUntil_ImmediateSuccess(t *testing.T) {
	err := waitUntil(context.Background(), time.Second, func() (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
}

func TestWaitUntil_EventualSuccess(t *testing.T) {
	calls := 0
	err := waitUntil(context.Background(), time.Millisecond, func() (bool, error) {
		calls++
		return calls >= 3, nil
	})
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	if calls != 3 {
		t.Errorf("calls: got %d, want 3", calls)
	}
}

func TestWaitUntil_FnError(t *testing.T) {
	want := errors.New("boom")
	err := waitUntil(context.Background(), time.Millisecond, func() (bool, error) {
		return false, want
	})
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func TestWaitUntil_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitUntil(ctx, time.Hour, func() (bool, error) {
		return false, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestWaitUntil_FnErrorBeforeContextCancel(t *testing.T) {
	want := errors.New("fn error")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := waitUntil(ctx, time.Hour, func() (bool, error) {
		return false, want
	})
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}
