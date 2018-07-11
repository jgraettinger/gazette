package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"github.com/LiveRamp/gazette/pkg/client"
	"github.com/LiveRamp/gazette/pkg/fragment"
	"github.com/LiveRamp/gazette/pkg/http_gateway"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
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
	// BrokerSpec to advertise within the Gazette cluster.
	Spec pb.BrokerSpec
	// Metrics collection and reporting configuration.
	Metrics struct {
		Port uint16
		Path string
	}
	// Logging configuration.
	Log struct {
		Level string
	}
	// Etcd client configuration.
	Etcd struct {
		Endpoint pb.Endpoint   // Address at which to reach Etcd.
		Root     string        // Root path in Etcd of the service. Eg, "/gazette/cluster".
		LeaseTTL time.Duration // TTL of established Etcd lease.
	}
	// gRPC server configuration.
	GRPC struct {
		KeepAlive keepalive.ServerParameters
	}
	// Cache configuration for LRU caches used by the broker.
	Caches struct {
		// Number of gRPC peer connections to cache. Caching of peer connections
		// reduces latency by amortizing initial gRPC connection setup time.
		DialerConns int
		// Number of journal routes the HTTP Gateway should cache. Caching enables
		// requests to be directly dispatched against the current broker, rather
		// than being proxied through the local gRPC server.
		HTTPGatewayRoutes int
	}
}

// Validate returns an error if the Config is not well-formed.
func (cfg Config) Validate() error {
	if err := cfg.Spec.Validate(); err != nil {
		return pb.ExtendContext(err, "Spec")
	} else if cfg.Metrics.Port == 0 {
		return pb.NewValidationError("invalid Metrics.Port: (%d; expected > 0)", cfg.Metrics.Port)
	} else if cfg.Metrics.Path == "" {
		return pb.NewValidationError("expected Metrics.Path")
	} else if cfg.Log.Level == "" {
		return pb.NewValidationError("expected Log.Level")
	} else if err := cfg.Etcd.Endpoint.Validate(); err != nil {
		return pb.ExtendContext(err, "Etcd.Endpoint")
	} else if !path.IsAbs(cfg.Etcd.Root) {
		return pb.NewValidationError("Etcd.Root not an absolute path: %s", cfg.Etcd.Root)
	} else if cfg.Etcd.LeaseTTL <= time.Second {
		return pb.NewValidationError("invalid Etcd.LeaseTTL (%s; expected >= %s)",
			cfg.Etcd.LeaseTTL, time.Second)
	} else if cfg.Caches.DialerConns <= 0 {
		return pb.NewValidationError("invalid Caches.DialerConns (%d; expected >= 0)",
			cfg.Caches.DialerConns)
	} else if cfg.Caches.HTTPGatewayRoutes <= 0 {
		return pb.NewValidationError("invalid Caches.HTTPGatewayRoutes (%d; expected >= 0)",
			cfg.Caches.HTTPGatewayRoutes)
	}
	return nil
}

func main() {
	defer mainboilerplate.LogPanic()
	grpc.EnableTracing = true

	var cfg Config
	mainboilerplate.MustParseConfig(configFile, "gazette-config", &cfg)

	prometheus.MustRegister(metrics.GazetteCollectors()...)
	mainboilerplate.InitMetrics(fmt.Sprintf(":%d", cfg.Metrics.Port), cfg.Metrics.Path)
	mainboilerplate.InitLog(cfg.Log.Level)

	var etcd, session = buildEtcdSession(&cfg)
	defer session.Close()

	// Bind listeners before we advertise our endpoint.
	var cMux, grpcL, httpL = buildListeners(&cfg)

	var ks = pb.NewKeySpace(cfg.Etcd.Root)
	var memberKey = v3_allocator.MemberKey(ks, cfg.Spec.Id.Zone, cfg.Spec.Id.Suffix)

	announcement, err := v3_allocator.Announce(context.Background(),
		etcd, memberKey, cfg.Spec.MarshalString(), session.Lease())
	must(err, "failed to announce member key", "key", memberKey)

	// Install a signal handler which zeros our advertised JournalLimit.
	// Upon seeing this, Allocator will work to discharge all of our assigned
	// Items, and Allocate will exit gracefully when none remain.
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		var sig = <-signalCh
		log.WithField("signal", sig).Info("caught signal")

		// Write an updated advertisement of our spec, dropping the JournalLimit to zero.
		cfg.Spec.JournalLimit = 0
		if err := announcement.Update(context.Background(), cfg.Spec.MarshalString()); err != nil {
			log.WithField("err", err).Error("failed to update member announcement")
		}
	}()

	dialer, err := client.NewDialer(cfg.Caches.DialerConns)
	must(err, "failed to create Dialer")
	loopback, err := dialer.DialEndpoint(context.Background(), cfg.Spec.Endpoint)
	must(err, "failed to dial local broker")

	var state = v3_allocator.NewObservedState(ks, memberKey)
	var service = broker.NewService(state, dialer, pb.NewBrokerClient(loopback), etcd)
	var persister = fragment.NewPersister()
	broker.SetSharedPersister(persister)

	go persister.Serve()

	must(ks.Load(context.Background(), etcd, announcement.Revision), "failed to load KeySpace")

	go func() {
		must(ks.Watch(context.Background(), etcd), "KeySpace Watch failed")
	}()
	go func() {
		must(serveHTTPGateway(&cfg, httpL, dialer), "gateway http.Serve failed")
	}()
	go func() {
		must(serveGRPC(&cfg, grpcL, service), "grpc Serve failed")
	}()
	go func() {
		must(cMux.Serve(), "cMux.Serve failed")
	}()

	if err = v3_allocator.Allocate(v3_allocator.AllocateArgs{
		Context: context.Background(),
		Etcd:    etcd,
		State:   state,
	}); err != nil {
		log.WithField("err", err).Error("Allocate failed")
	}

	persister.Finish()
	log.Info("goodbye")
}

func buildListeners(cfg *Config) (mux cmux.CMux, grpc, http net.Listener) {
	// Bind the specified listener. Use cmux to multiplex both HTTP and gRPC
	// sessions over the same port.
	var raw, err = net.Listen("tcp", cfg.Spec.Endpoint.URL().Host)
	must(err, "failed to bind listener")

	log.WithField("endpoint", cfg.Spec.Endpoint).Info("listening on endpoint")

	mux = cmux.New(keepalive2.TCPListener{TCPListener: raw.(*net.TCPListener)})
	grpc = mux.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	http = mux.Match(cmux.HTTP1Fast())

	return
}

func buildEtcdSession(cfg *Config) (etcd *clientv3.Client, session *concurrency.Session) {
	var err error
	etcd, err = clientv3.NewFromURL(string(cfg.Etcd.Endpoint))
	must(err, "failed to init etcd client")

	session, err = concurrency.NewSession(etcd, concurrency.WithTTL(int(cfg.Etcd.LeaseTTL.Seconds())))
	must(err, "failed to establish etcd lease session")
	return
}

func serveHTTPGateway(cfg *Config, l net.Listener, dialer client.Dialer) error {
	var loopback, err = dialer.DialEndpoint(context.Background(), cfg.Spec.Endpoint)
	must(err, "failed to dial local broker")

	routingClient, err := client.NewRoutingClient(pb.NewBrokerClient(loopback),
		cfg.Spec.Id.Zone, dialer, cfg.Caches.HTTPGatewayRoutes)
	must(err, "failed to build RoutingClient")

	return http.Serve(l, http_gateway.NewGateway(routingClient))
}

func serveGRPC(cfg *Config, l net.Listener, srv pb.BrokerServer) error {
	var grpcSrv = grpc.NewServer(grpc.KeepaliveParams(cfg.GRPC.KeepAlive))
	pb.RegisterBrokerServer(grpcSrv, srv)

	return grpcSrv.Serve(l)
}

func must(err error, msg string, extra ...interface{}) {
	if err == nil {
		return
	}

	var f = log.Fields{"err": err}
	for i := 0; i+1 < len(extra); i += 2 {
		f[extra[i].(string)] = extra[i+1]
	}
	log.WithFields(f).Fatal(msg)
}
