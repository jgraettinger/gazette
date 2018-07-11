package cmd

import (
	"flag"
	"fmt"
	"os"
	"strings"

	etcd "github.com/coreos/etcd/client"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
)

var configFile string

// Execute evaluates provided arguments against the rootCmd hierarchy.
// This is called by main.main().
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	for _, h := range shutdownHooks {
		h()
	}
}

// rootCmd parents all other commands in the hierarchy.
var rootCmd = &cobra.Command{
	Use:   "gazctl",
	Short: "gazctl is a command-line interface for interacting with gazette",
}

func init() {
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

func etcdClient() etcd.Client {
	if lazyEtcdClient == nil {
		var ep = viper.GetString("etcd.endpoint")
		if ep == "" {
			log.Fatal("etcd.endpoint not provided")
		}

		var err error
		lazyEtcdClient, err = etcd.New(etcd.Config{Endpoints: []string{ep}})
		if err != nil {
			log.WithField("err", err).Fatal("building etcd client")
		}
	}
	return lazyEtcdClient
}

func etcd3Client() *clientv3.Client {
	if lazyEtcd3Client == nil {
		var ep = viper.GetString("etcd.endpoint")
		if ep == "" {
			log.Fatal("etcd.endpoint not provided")
		}

		var err error
		lazyEtcd3Client, err = clientv3.NewFromURL(ep)
		if err != nil {
			log.WithField("err", err).Fatal("building etcd3 client")
		}
	}
	return lazyEtcd3Client
}

func cloudFS() cloudstore.FileSystem {
	if lazyCFS == nil {
		var cfs, err = cloudstore.NewFileSystem(nil, viper.GetString("cloud.fs.url"))
		if err != nil {
			log.WithField("err", err).Fatal("cannot initialize cloud filesystem")
		}
		lazyCFS = cfs
	}
	return lazyCFS
}

func userConfirms(message string) {
	if defaultYes {
		return
	}
	fmt.Println(message)
	fmt.Print("Confirm (y/N): ")

	var response string
	fmt.Scanln(&response)

	for _, opt := range []string{"y", "yes"} {
		if strings.ToLower(response) == opt {
			return
		}
	}
	log.Fatal("aborted by user")
}

var (
	lazyCFS         cloudstore.FileSystem
	lazyEtcdClient  etcd.Client
	lazyEtcd3Client *clientv3.Client

	defaultYes    bool
	shutdownHooks []func()
)
