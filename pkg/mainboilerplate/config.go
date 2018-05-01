// Package config provides building blocks and utilities for defining and parsing configuration file.
package mainboilerplate

import (
	"flag"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// MustParseConfig loads the config provided by |configPathFlag|, or if not
// found, searches for |configName| in the current directory. It binds to
// environment variables, and extracts & validates into the provided |config|.
func MustParseConfig(configPathFlag *string, configName string, config validator) {
	flag.Parse()

	if *configPathFlag != "" {
		viper.SetConfigFile(*configPathFlag)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName(configName)
	}

	if err := viper.ReadInConfig(); err != nil {
		log.WithField("err", err).Fatal("failed to read config")
	} else {
		log.WithField("path", viper.ConfigFileUsed()).Info("read config")
	}

	// Allow environment variables to override file configuration.
	// Treat variable underscores as nested-path specifiers.
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.Unmarshal(&config); err != nil {
		log.WithField("err", err).Fatal("failed to unmarshal")
	} else if err := config.Validate(); err != nil {
		viper.Debug()
		log.WithFields(log.Fields{"err": err, "cfg": config, "env": os.Environ()}).Fatal("config validation failed")
	}
}

type validator interface {
	Validate() error
}
