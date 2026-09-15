package harness

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// EnvFileName is the env file loaded automatically from the config directory.
const EnvFileName = "orca.env"

var varNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// applyVariables exports the benchmark variables into the process environment
// so that ${NAME} references and child processes see them. Precedence, highest
// first: the existing process environment, extraFiles in order (later files
// win), orca.env next to the config file, and the defaults in vars.
func applyVariables(vars map[string]string, configPath string, extraFiles []string) error {
	for name := range vars {
		if !varNameRE.MatchString(name) {
			return fmt.Errorf("variables: invalid name %q", name)
		}
	}
	merged := map[string]string{}
	for k, v := range vars {
		merged[k] = v
	}
	auto := filepath.Join(filepath.Dir(configPath), EnvFileName)
	files := []string{}
	if _, err := os.Stat(auto); err == nil {
		files = append(files, auto)
	}
	files = append(files, extraFiles...)
	for _, f := range files {
		vars, err := parseEnvFile(f)
		if err != nil {
			return err
		}
		for k, v := range vars {
			merged[k] = v
		}
	}
	names := make([]string, 0, len(merged))
	for k := range merged {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if _, set := os.LookupEnv(k); set {
			continue
		}
		if err := os.Setenv(k, merged[k]); err != nil {
			return fmt.Errorf("set %s: %w", k, err)
		}
	}
	return nil
}

// parseEnvFile reads KEY=VALUE lines. Blank lines and lines starting with #
// are ignored, a leading "export " is dropped, and matching single or double
// quotes around the value are stripped.
func parseEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read env file: %w", err)
	}
	defer f.Close()
	vars := map[string]string{}
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, val, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || !varNameRE.MatchString(key) {
			return nil, fmt.Errorf("%s:%d: expected KEY=VALUE", path, n)
		}
		val = strings.TrimSpace(val)
		if len(val) >= 2 && (val[0] == '"' || val[0] == '\'') && val[len(val)-1] == val[0] {
			val = val[1 : len(val)-1]
		}
		vars[key] = val
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read env file %s: %w", path, err)
	}
	return vars, nil
}
