package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/LiveRamp/gazette/pkg/client"
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
	Caches struct {
		DialerConns       int
		HTTPGatewayRoutes int
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

func main() {
	defer mainboilerplate.LogPanic()

	var cfg Config
	mainboilerplate.MustParseConfig(configFile, "gazette-config", &cfg)

	prometheus.MustRegister(metrics.GazetteCollectors()...)
	mainboilerplate.InitMetrics(strconv.Itoa(int(cfg.Metrics.Port)), cfg.Metrics.Path)
	mainboilerplate.InitLog(cfg.Log.Level)

	var etcd, session = buildEtcdSession(&cfg)
	defer session.Close()

	// Bind listeners before we advertise our endpoint.
	var cMux, grpcL, httpL = buildListeners(&cfg)

	var ks = broker.NewKeySpace(cfg.Etcd.Root)
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

	var state = v3_allocator.NewObservedState(ks, memberKey)
	var service = broker.NewService(dialer, state)

	must(ks.Load(context.Background(), etcd, announcement.Revision), "failed to load KeySpace")

	go must(ks.Watch(context.Background(), etcd), "KeySpace Watch failed")
	go must(serveHTTPGateway(&cfg, httpL, dialer), "gateway http.Serve failed")
	go must(serveGRPC(&cfg, grpcL, &service), "grpc Serve failed")
	must(cMux.Serve(), "cMux.Serve failed")
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

/*
	var advertiseTestJournal = func() {
		var spec = pb.JournalSpec{
			Name:        "foo/bar",
			Replication: 1,
			Fragment: pb.JournalSpec_Fragment{
				Length:           1 << 16,
				CompressionCodec: pb.CompressionCodec_GZIP,
				RefreshInterval:  time.Second,
			},
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

*/
