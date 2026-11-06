package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"Tuna_v2/config"
	"Tuna_v2/logger"
	"Tuna_v2/node"
)

func main() {
	if err := command().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func command() *cobra.Command {
	root := &cobra.Command{
		Use:           "tuna",
		Short:         "Tuna_v2 DAG consensus",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	var keyPath string
	var keyNodes int

	keys := &cobra.Command{
		Use:   "keys",
		Short: "Generate Ed25519 node key pairs",
		RunE: func(_ *cobra.Command, _ []string) error {
			if keyNodes < 1 {
				return fmt.Errorf("nodes must be positive")
			}

			if err := os.MkdirAll(keyPath, 0o700); err != nil {
				return err
			}

			config.GenerateKeyFiles(keyNodes, keyPath)
			return nil
		},
	}
	keys.Flags().StringVar(&keyPath, "path", ".", "Key output directory")
	keys.Flags().IntVar(&keyNodes, "nodes", 4, "Number of key pairs")

	var thresholdPath string
	var n, t int

	threshold := &cobra.Command{
		Use:   "threshold_keys",
		Short: "Generate optional threshold signature keys",
		RunE: func(_ *cobra.Command, _ []string) error {
			if n < 1 || t < 1 || t > n {
				return fmt.Errorf("require 1 <= T <= N")
			}

			if err := os.MkdirAll(thresholdPath, 0o700); err != nil {
				return err
			}

			config.GenerateTsKeyFiles(n, t, thresholdPath)
			return nil
		},
	}
	threshold.Flags().StringVar(&thresholdPath, "path", ".", "Key output directory")
	threshold.Flags().IntVar(&n, "N", 4, "Number of shares")
	threshold.Flags().IntVar(&t, "T", 3, "Recovery threshold")

	var genesisPath string
	var genesisNodes, port int

	genesis := &cobra.Command{
		Use:   "genesis",
		Short: "Generate matching local keys, committee and parameters",
		RunE: func(_ *cobra.Command, _ []string) error {
			return config.GenerateLocalFiles(genesisNodes, port, genesisPath)
		},
	}
	genesis.Flags().StringVar(&genesisPath, "path", ".", "Configuration directory")
	genesis.Flags().IntVar(&genesisNodes, "nodes", 4, "Committee size")
	genesis.Flags().IntVar(&port, "port", 9000, "First port; reserves 2*nodes ports")

	var keyFile, thresholdFile, committeeFile, parametersFile, storePath, logPath string
	var level, id int

	run := &cobra.Command{
		Use:   "run",
		Short: "Run a Tuna_v2 node",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			instance, err := node.NewNode(keyFile, thresholdFile, committeeFile, parametersFile, storePath, logPath, level, id)
			if err != nil {
				return err
			}

			return instance.RunContext(ctx)
		},
	}
	run.Flags().StringVar(&keyFile, "keys", "", "Node Ed25519 key file")
	run.Flags().StringVar(&thresholdFile, "threshold_keys", "", "Optional threshold key file")
	run.Flags().StringVar(&committeeFile, "committee", "", "Committee JSON file")
	run.Flags().StringVar(&parametersFile, "parameters", "", "Parameter JSON file")
	run.Flags().StringVar(&storePath, "store", "", "Persistent store directory")
	run.Flags().StringVar(&logPath, "log_out", ".", "Log directory")
	run.Flags().IntVar(&level, "log_level", int(logger.DeployLevel), "Log level bitmask")
	run.Flags().IntVar(&id, "node_id", 0, "Local committee node ID")
	for _, flag := range []string{
		"keys",
		"committee",
		"parameters",
		"store",
	} {
		run.MarkFlagRequired(flag)
	}
	// Keep the existing deployment entry point, backed by the local configuration generator.
	deploy := &cobra.Command{
		Use:   "deploy",
		Short: "Generate a local testbed configuration",
		RunE:  genesis.RunE,
	}
	deploy.Flags().StringVar(&genesisPath, "path", ".", "Configuration directory")
	deploy.Flags().IntVar(&genesisNodes, "nodes", 4, "Committee size")
	deploy.Flags().IntVar(&port, "port", 9000, "First local port")
	root.AddCommand(keys, threshold, genesis, run, deploy)
	return root
}
