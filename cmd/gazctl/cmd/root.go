package cmd

import (
	"flag"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/LiveRamp/gazette/cmd/gazctl/cmd/internal"
	"github.com/LiveRamp/gazette/cmd/gazctl/cmd/journal"
	"github.com/LiveRamp/gazette/cmd/gazctl/cmd/shard"
)

var configFile string

// Execute evaluates provided arguments against the rootCmd hierarchy.
// This is called by main.main().
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	for _, h := range internal.ShutdownHooks {
		h()
	}
}

// rootCmd parents all other commands in the hierarchy.
var rootCmd = &cobra.Command{
	Use:   "gazctl",
	Short: "gazctl is a command-line interface for interacting with gazette",
}

func init() {
	rootCmd.AddCommand(journal.Cmd)
	rootCmd.AddCommand(shard.Cmd)

	log.SetOutput(os.Stderr)

	cobra.OnInitialize(initConfig)
	flag.Parse()

	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "f", "",
		"config file (default is $HOME/.gazctl.yaml)")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if configFile != "" {
		viper.SetConfigFile(configFile)
	}

	viper.SetConfigName(".gazctl")
	viper.AddConfigPath("$HOME")

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.WithField("file", viper.ConfigFileUsed()).Info("read config")
	}

	// Allow environment variables to override file configuration.
	// Treat variable underscores as nested-path specifiers.
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}
