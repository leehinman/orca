package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:           "orca",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(runCmd(), upCmd(), benchCmd(), downCmd(), validateCmd())
	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "orca: %v\n", err)
		os.Exit(1)
	}
}

func runCmd() *cobra.Command {
	var configPath string
	var envFiles []string
	var jsonOutput bool
	var skipTeardown bool

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the full benchmark lifecycle: up, bench, down",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := harness.LoadConfig(configPath, envFiles...)
			if err != nil {
				return err
			}

			r := harness.NewRunner()
			registerFactories(r, cfg)

			runErr := r.Run(context.Background(), cfg, harness.RunOptions{SkipTeardown: skipTeardown})
			writeSummary(cfg, r.State(), jsonOutput, runErr)
			return runErr
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "config.yaml", "path to benchmark config file")
	addEnvFileFlag(cmd, &envFiles)
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "print results as JSON instead of a text table")
	cmd.Flags().BoolVar(&skipTeardown, "skip-teardown", false, "skip teardown and leave the run for a later orca down")
	return cmd
}

func upCmd() *cobra.Command {
	var configPath string
	var envFiles []string

	cmd := &cobra.Command{
		Use:   "up",
		Short: "Start a new run: setup, configure, seed",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := harness.LoadConfig(configPath, envFiles...)
			if err != nil {
				return err
			}

			r := harness.NewRunner()
			registerFactories(r, cfg)

			return r.Up(context.Background(), cfg)
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "config.yaml", "path to benchmark config file")
	addEnvFileFlag(cmd, &envFiles)
	return cmd
}

func benchCmd() *cobra.Command {
	var configPath string
	var envFiles []string
	var runDir string
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Benchmark the current run: warmup, execute, collect, report",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := harness.LoadConfigUnchecked(configPath, envFiles...)
			if err != nil {
				return err
			}
			state, err := loadRun(runDir)
			if err != nil {
				return err
			}

			r := harness.NewRunner()
			registerFactories(r, cfg)

			benchErr := r.Bench(context.Background(), cfg, state)
			writeSummary(cfg, r.State(), jsonOutput, benchErr)
			return benchErr
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "config.yaml", "path to benchmark config file")
	addEnvFileFlag(cmd, &envFiles)
	cmd.Flags().StringVar(&runDir, "run", "", "run directory (default: the current run)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "print results as JSON instead of a text table")
	return cmd
}

func downCmd() *cobra.Command {
	var configPath string
	var envFiles []string
	var runDir string

	cmd := &cobra.Command{
		Use:     "down",
		Aliases: []string{"teardown"},
		Short:   "Tear down the current run",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := harness.LoadConfigUnchecked(configPath, envFiles...)
			if err != nil {
				return err
			}
			state, err := loadRun(runDir)
			if err != nil {
				return err
			}

			r := harness.NewRunner()
			registerFactories(r, cfg)

			return r.Down(context.Background(), cfg, state)
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "config.yaml", "path to benchmark config file")
	addEnvFileFlag(cmd, &envFiles)
	cmd.Flags().StringVar(&runDir, "run", "", "run directory (default: the current run)")
	return cmd
}

func addEnvFileFlag(cmd *cobra.Command, envFiles *[]string) {
	cmd.Flags().StringArrayVar(envFiles, "env-file", nil, "KEY=VALUE file applied after "+harness.EnvFileName+" (repeatable, later files win)")
}

func loadRun(runDir string) (*harness.State, error) {
	if runDir == "" {
		var err error
		if runDir, err = harness.CurrentRun(); err != nil {
			return nil, err
		}
	}
	return harness.LoadRun(runDir)
}

func writeSummary(cfg *harness.Config, state *harness.State, jsonOutput bool, runErr error) {
	if state == nil {
		return
	}
	res := cfg.BuildResult(state)
	if runErr != nil {
		res.Error = runErr.Error()
	}
	w := io.Writer(os.Stdout)
	if resultsDir, err := state.Resolve("${results}"); err == nil {
		if f, err := os.Create(filepath.Join(resultsDir, "summary.txt")); err == nil {
			defer f.Close()
			w = io.MultiWriter(os.Stdout, f)
		}
	}
	if jsonOutput {
		res.JSON(w) //nolint
	} else {
		res.Print(w)
	}
}

func validateCmd() *cobra.Command {
	var configPath string
	var envFiles []string

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate a benchmark config file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := harness.LoadConfig(configPath, envFiles...); err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, "ok")
			return nil
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "config.yaml", "path to benchmark config file")
	addEnvFileFlag(cmd, &envFiles)
	return cmd
}

func registerFactories(r *harness.Runner, cfg *harness.Config) {
	r.Register("noop", steps.NewNoopFactory())
	r.Register("wait", steps.NewWaitFactory())
	r.Register("write-ini", steps.NewWriteIniFactory())
	r.Register("kind", steps.NewKindFactory())
	r.Register("kind-delete", steps.NewKindDeleteFactory())
	r.Register("kind-load", steps.NewKindLoadFactory())
	r.Register("kubectl", steps.NewKubectlFactory())
	r.Register("wait-pods", steps.NewWaitPodsFactory())
	r.Register("watch-job", steps.NewWatchJobFactory())
	r.Register("tsv-derive", steps.NewTsvDeriveFactory())
	r.Register("chart", steps.NewChartFactory())
	r.Register("shell", steps.NewShellFactory())
	r.Register("opentofu", steps.NewOpenTofuFactory())
	r.Register("ansible", steps.NewAnsibleFactory())
	r.Register("es-doc-count", steps.NewEsDocCountFactory(cfg.Elasticsearch))
	r.Register("es-log-span", steps.NewEsLogSpanFactory(cfg.Elasticsearch))
	r.Register("es-agg", steps.NewEsAggFactory(cfg.Elasticsearch))
	r.Register("es-tsv", steps.NewEsTsvFactory(cfg.Elasticsearch))
	r.Register("elastic-agent-metrics", steps.NewEsAgentMetricsFactory(cfg.Elasticsearch, cfg.Kibana))
	r.Register("elasticsearch-metrics", steps.NewEsMetricsFactory(cfg.Elasticsearch))
	r.Register("stack-versions", steps.NewStackVersionsFactory(cfg.Elasticsearch, cfg.Kibana))
	r.Register("fleet-agent-diagnostics", steps.NewFleetDiagFactory(cfg.Kibana))
}
