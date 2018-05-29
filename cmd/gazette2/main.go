package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/soheilhy/cmux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/LiveRamp/gazette/pkg/broker"
	keepalive2 "github.com/LiveRamp/gazette/pkg/keepalive"
	"github.com/LiveRamp/gazette/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/pkg/metrics"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

var configFile = flag.String("config", "", "Path to configuration file. "+
	"Defaults to `gazette-config.{toml|yaml|json}` in the current working directory.")

type Config struct {
	Spec pb.BrokerSpec

	Metrics struct {
		Port uint16
		Path string
	}
	Log struct {
		Level string
	}
	Etcd struct {
		Endpoint string        // Address at which to reach Etcd.
		Root     string        // Root path in Etcd of the service. Eg, "/gazette/cluster".
		LeaseTTL time.Duration // TTL of established Etcd lease.
	}
	GRPC struct {
		KeepAlive keepalive.ServerParameters
	}
}

func (cfg Config) Validate() error {
	if err := cfg.Spec.Validate(); err != nil {
		return pb.ExtendContext(err, "Spec")
	}

	if cfg.Metrics.Port == 0 {
		return pb.NewValidationError("invalid metrics port: (%d; expected > 0)", cfg.Metrics.Port)
	}

	/*
		if !path.IsAbs(cfg.Service.AllocatorRoot) {
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
	*/
	return nil
}

func main() {
	//defer mainboilerplate.LogPanic()

	var cfg Config
	mainboilerplate.MustParseConfig(configFile, "gazette-config", &cfg)

	prometheus.MustRegister(metrics.GazetteCollectors()...)
	mainboilerplate.InitMetrics(strconv.Itoa(int(cfg.Metrics.Port)), cfg.Metrics.Path)
	//mainboilerplate.InitLog(cfg.Log.Level)
	//gensupport.RegisterHook(traceRequests)

	var ctx = context.Background()

	// Bind the specified listener. Use cmux to multiplex both HTTP and gRPC
	// sessions over the same port.
	var listener net.Listener

	if u, err := url.Parse(cfg.Spec.Endpoint); err != nil {
		log.WithField("err", err).Fatal("failed to parse Spec.Endpoint") // Also covered by cfg.Validate.
	} else if listener, err = net.Listen("tcp", u.Host); err != nil {
		log.WithField("err", err).Fatal("failed to bind listener")
	}

	var lMux = cmux.New(keepalive2.TCPListener{listener.(*net.TCPListener)})
	var grpcL = lMux.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	var httpL = lMux.Match(cmux.HTTP1Fast())
	log.WithField("endpoint", cfg.Spec.Endpoint).Info("listening on endpoint")

	// Connect to Etcd, and establish a lease having the same lifetime as the broker.
	etcd, err := clientv3.NewFromURL(cfg.Etcd.Endpoint)
	if err != nil {
		log.WithField("err", err).Fatal("failed to init etcd client")
	}
	lease, err := etcd.Lease.Grant(ctx, int64(cfg.Etcd.LeaseTTL.Seconds()))
	if err != nil {
		log.WithField("err", err).Fatal("failed to obtain etcd lease")
	} else if _, err = etcd.Lease.KeepAlive(ctx, lease.ID); err != nil {
		log.WithField("err", err).Fatal("failed to begin Lease KeepAlive")
	}
	log.WithField("lease", lease).Info("acquired etcd lease")

	defer func() {
		if _, err := etcd.Lease.Revoke(ctx, lease.ID); err != nil {
			log.WithField("err", err).Warn("failed to remove lease")
		}
	}()

	var ks = newBrokerKeySpace(cfg.Etcd.Root)
	var localKey = v3_allocator.MemberKey(ks, cfg.Spec.Id.Zone, cfg.Spec.Id.Suffix)

	var advertiseTestJournal = func() {
		var spec = pb.JournalSpec{
			Name:             "foo/bar",
			Replication:      1,
			FragmentLength:   1 << 16,
			TransactionSize:  1 << 12,
			CompressionCodec: pb.CompressionCodec_GZIP,
			Labels: pb.LabelSet{
				Labels: []pb.Label{
					{Name: "label-key", Value: "label-value"},
					{Name: "topic", Value: "foo"},
				},
			},
		}
		if err = spec.Validate(); err != nil {
			log.WithField("err", err).Fatal("bad journal spec")
		}

		if _, err = etcd.Put(ctx,
			v3_allocator.ItemKey(ks, spec.Name.String()),
			spec.MarshalString(),
		); err != nil {
			log.WithField("err", err).Fatal("failed to write JournalSpec to etcd")
		}
	}
	advertiseTestJournal()

	// Advertise the configured BrokerSpec within Etcd, under our lease.
	var advertiseSpec = func() {
		if _, err = etcd.Put(ctx,
			localKey,
			cfg.Spec.MarshalString(),
			clientv3.WithLease(lease.ID),
		); err != nil {
			log.WithField("err", err).Fatal("failed to write BrokerSpec to etcd")
		}
	}
	advertiseSpec()

	// Install a signal handler, which will zero our advertised JournalLimit.
	// Upon seeing this, Allocator will work to discharge all of our assigned
	// Items, and Serve will exit gracefully when none remain.
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		var sig = <-signalCh
		log.WithField("signal", sig).Info("caught signal")

		// Write an updated advertisement of our spec, dropping the JournalLimit to zero.
		cfg.Spec.JournalLimit = 0
		advertiseSpec()
	}()

	var b = broker.NewRouter(ks, cfg.Spec.Id, etcd)

	var grpcSrv = grpc.NewServer(grpc.KeepaliveParams(cfg.GRPC.KeepAlive))
	pb.RegisterBrokerServer(grpcSrv, b)

	var httpAPI = broker.NewHTTPGateway(b)

	// Start serving over GRPC.
	go func() {
		if err := grpcSrv.Serve(grpcL); err != nil {
			log.WithField("err", err).Fatal("grpcSrv.Serve failed")
		}
	}()
	// Start serving over HTTP.
	go func() {
		if err := http.Serve(httpL, httpAPI); err != nil {
			log.WithField("err", err).Fatal("http.Serve failed")
		}
	}()
	// Start accepting connections over the listener.
	go func() {
		if err := lMux.Serve(); err != nil {
			log.WithField("err", err).Fatal("lMux.Serve failed")
		}
	}()

	var alloc = v3_allocator.Allocator{
		KeySpace:           ks,
		LocalKey:           localKey,
		LocalItemsCallback: b.UpdateLocalItems,
	}

	if err = alloc.Serve(ctx, etcd); err != nil {
		log.WithField("err", err).Fatal("Allocator.Serve exited with error")
	}
}
