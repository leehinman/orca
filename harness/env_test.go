package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func unsetForTest(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		if v, ok := os.LookupEnv(n); ok {
			t.Cleanup(func() { os.Setenv(n, v) }) //nolint:errcheck
		} else {
			t.Cleanup(func() { os.Unsetenv(n) }) //nolint:errcheck
		}
		os.Unsetenv(n) //nolint:errcheck
	}
}

func TestParseEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.env")
	os.WriteFile(path, []byte(`
# comment
PLAIN=one
export EXPORTED=two
DQ="three words"
SQ='four'
SPACED = five
EMPTY=
`), 0o644) //nolint:errcheck
	got, err := parseEnvFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"PLAIN": "one", "EXPORTED": "two", "DQ": "three words", "SQ": "four", "SPACED": "five", "EMPTY": ""}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: got %q, want %q", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d entries, want %d: %v", len(got), len(want), got)
	}
	os.WriteFile(path, []byte("NOT A LINE\n"), 0o644) //nolint:errcheck
	if _, err := parseEnvFile(path); err == nil || !strings.Contains(err.Error(), ":1:") {
		t.Errorf("expected line error, got %v", err)
	}
}

func TestLoadConfig_VariablesPrecedence(t *testing.T) {
	unsetForTest(t, "ORCA_T_DEFAULT", "ORCA_T_AUTO", "ORCA_T_EXTRA", "ORCA_T_EXTRA2", "ORCA_T_PROC", "ORCA_T_UNDECLARED")
	t.Setenv("ORCA_T_PROC", "from-process")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
benchmark:
  name: bench
variables:
  ORCA_T_DEFAULT: d
  ORCA_T_AUTO: d
  ORCA_T_EXTRA: d
  ORCA_T_EXTRA2: d
  ORCA_T_PROC: d
phases:
  setup:
    - shell:
        run: "echo ${ORCA_T_DEFAULT} ${ORCA_T_UNDECLARED}"
`), 0o644) //nolint:errcheck
	os.WriteFile(filepath.Join(dir, EnvFileName), []byte("ORCA_T_AUTO=auto\nORCA_T_EXTRA=auto\nORCA_T_EXTRA2=auto\nORCA_T_PROC=auto\nORCA_T_UNDECLARED=auto\n"), 0o644) //nolint:errcheck
	e1 := filepath.Join(dir, "a.env")
	e2 := filepath.Join(dir, "b.env")
	os.WriteFile(e1, []byte("ORCA_T_EXTRA=e1\nORCA_T_EXTRA2=e1\n"), 0o644) //nolint:errcheck
	os.WriteFile(e2, []byte("ORCA_T_EXTRA2=e2\n"), 0o644)                  //nolint:errcheck

	if _, err := LoadConfig(cfgPath, e1, e2); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	for k, want := range map[string]string{
		"ORCA_T_DEFAULT":    "d",
		"ORCA_T_AUTO":       "auto",
		"ORCA_T_EXTRA":      "e1",
		"ORCA_T_EXTRA2":     "e2",
		"ORCA_T_PROC":       "from-process",
		"ORCA_T_UNDECLARED": "auto",
	} {
		if got := os.Getenv(k); got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}
}

func TestLoadConfig_UndeclaredUnsetStillFails(t *testing.T) {
	unsetForTest(t, "ORCA_T_MISSING")
	path := writeTempConfig(t, `
benchmark:
  name: bench
variables:
  ORCA_T_OTHER: x
phases:
  setup:
    - shell:
        run: "echo ${ORCA_T_MISSING}"
`)
	unsetForTest(t, "ORCA_T_OTHER")
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "ORCA_T_MISSING") {
		t.Fatalf("expected unset env var error, got %v", err)
	}
}

func TestLoadConfig_InvalidVariableName(t *testing.T) {
	path := writeTempConfig(t, `
benchmark:
  name: bench
variables:
  "BAD-NAME": x
`)
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "invalid name") {
		t.Fatalf("expected invalid name error, got %v", err)
	}
}

func TestLoadConfigUnchecked_AppliesVariables(t *testing.T) {
	unsetForTest(t, "ORCA_T_BENCH")
	path := writeTempConfig(t, `
benchmark:
  name: bench
variables:
  ORCA_T_BENCH: "500"
`)
	if _, err := LoadConfigUnchecked(path); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("ORCA_T_BENCH"); got != "500" {
		t.Errorf("got %q, want 500", got)
	}
}
