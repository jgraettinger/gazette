package main

import (
	"flag"
	"fmt"
	"path"
	"plugin"

	etcd "github.com/coreos/etcd/client"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/gazette"
	"github.com/LiveRamp/gazette/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/pkg/metrics"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

var configFile = flag.String("config", "", "Path to configuration file. "+
	"Defaults to `consumer-config.{toml|yaml|json}` in the current working directory.")

type Config struct {
	// Metrics collection and reporting configuration.
	Metrics struct {
		Port uint16
		Path string
	}
	// Logging configuration.
	Log struct {
		Level string
	}
	Service struct {
		AllocatorRoot   string // Absolute path in Etcd of the service consensus.Allocator.
		LocalRouteKey   string // Unique key of this consumer instance. By convention, this is bound "host:port" address.
		Plugin          string // Path of consumer plugin to load & run.
		RecoveryLogRoot string // Path prefix for the consumer's recovery-log Journals.
		ShardStandbys   uint8  // Number of warm-standby replicas to allocate for each Consumer shard.
		Workdir         string // Local directory for ephemeral serving files.
	}
	Etcd    struct{ Endpoint string } // Etcd endpoint to use.
	Gazette struct{ Endpoint string } // Gazette endpoint to use.
}

func (cfg Config) Validate() error {
	if cfg.Metrics.Port == 0 {
		return pb.NewValidationError("invalid Metrics.Port: (%d; expected > 0)", cfg.Metrics.Port)
	} else if cfg.Metrics.Path == "" {
		return pb.NewValidationError("expected Metrics.Path")
	} else if cfg.Log.Level == "" {
		return pb.NewValidationError("expected Log.Level")
	} else if !path.IsAbs(cfg.Service.AllocatorRoot) {
		return fmt.Errorf("Service.AllocatorRoot not an absolute path: %s", cfg.Service.AllocatorRoot)
	} else if cfg.Service.RecoveryLogRoot == "" {
		return fmt.Errorf("Service.RecoveryLogRoot not specified")
	} else if cfg.Service.Workdir == "" {
		return fmt.Errorf("Service.Workdir not specified")
	} else if cfg.Service.LocalRouteKey == "" {
		return fmt.Errorf("Service.LocalRouteKey not specified")
	} else if cfg.Etcd.Endpoint == "" {
		return fmt.Errorf("Etcd.Endpoint not specified")
	} else if cfg.Gazette.Endpoint == "" {
		return fmt.Errorf("Gazette.Endpoint not specified")
	}
	return nil
}

func main() {
	defer mainboilerplate.LogPanic()
	grpc.EnableTracing = true

	var cfg Config
	mainboilerplate.MustParseConfig(configFile, "consumer-cfg", &cfg)

	prometheus.MustRegister(metrics.GazetteClientCollectors()...)
	prometheus.MustRegister(metrics.GazetteConsumerCollectors()...)
	mainboilerplate.InitMetrics(fmt.Sprintf(":%d", cfg.Metrics.Port), cfg.Metrics.Path)
	mainboilerplate.InitLog(cfg.Log.Level)

	var module, err = plugin.Open(cfg.Service.Plugin)
	if err != nil {
		log.WithFields(log.Fields{"path": cfg.Service.Plugin, "err": err}).Fatal("failed to open plugin module")
	}
	flag.Parse() // Parse again to initialize any plugin flags.

	var instance consumer.Consumer
	if i, err := module.Lookup("Consumer"); err != nil {
		log.WithField("err", err).Fatal("failed to lookup Consumer symbol")
	} else if c, ok := i.(*consumer.Consumer); !ok {
		log.WithField("instance", i).Fatalf("symbol `Consumer` is not a consumer.Consumer: %#v", i)
	} else {
		instance = *c
	}

	etcdClient, err := etcd.New(etcd.Config{Endpoints: []string{cfg.Etcd.Endpoint}})
	if err != nil {
		log.WithField("err", err).Fatal("failed to init etcd client")
	}
	gazClient, err := gazette.NewClient(cfg.Gazette.Endpoint)
	if err != nil {
		log.WithField("err", err).Fatal("failed to init gazette client")
	}

	var writeService = gazette.NewWriteService(gazClient)
	writeService.Start()
	defer writeService.Stop() // Flush writes on exit.

	var runner = &consumer.Runner{
		Consumer:        instance,
		ConsumerRoot:    cfg.Service.AllocatorRoot,
		LocalDir:        cfg.Service.Workdir,
		LocalRouteKey:   cfg.Service.LocalRouteKey,
		RecoveryLogRoot: cfg.Service.RecoveryLogRoot,
		ReplicaCount:    int(cfg.Service.ShardStandbys),

		Etcd: etcdClient,
		Gazette: struct {
			*gazette.Client
			*gazette.WriteService
		}{gazClient, writeService},
	}

	if err = runner.Run(); err != nil {
		log.WithField("err", err).Error("runner.Run failed")
	}
}
