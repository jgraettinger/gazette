// +build !windows

package internal

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/url"
	"plugin"

	etcd "github.com/coreos/etcd/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/LiveRamp/gazette/pkg/consumer"
	"github.com/LiveRamp/gazette/pkg/gazette"
	"github.com/LiveRamp/gazette/pkg/recoverylog"
)

func ConsumerPlugin() consumer.Consumer {
	if lazyConsumerPlugin == nil {
		var path = viper.GetString("consumer.plugin")
		if path == "" {
			return nil
		}

		var module, err = plugin.Open(path)
		if err != nil {
			log.WithFields(log.Fields{"path": path, "err": err}).Fatal("failed to open plugin module")
		}

		if i, err := module.Lookup("Consumer"); err != nil {
			log.WithField("err", err).Fatal("failed to lookup Consumer symbol")
		} else if c, ok := i.(*consumer.Consumer); !ok {
			log.WithField("instance", i).Fatalf("symbol `Consumer` is not a consumer.Consumer: %#v", i)
		} else {
			lazyConsumerPlugin = *c
		}
	}
	return lazyConsumerPlugin
}

func GazetteClient() *gazette.Client {
	if lazyGazetteClient == nil {
		var ep = viper.GetString("gazette.endpoint")
		if ep == "" {
			log.Fatal("gazette.endpoint not provided")
		}

		var err error
		lazyGazetteClient, err = gazette.NewClient(ep)
		if err != nil {
			log.WithField("err", err).Fatal("building gazette client")
		}
	}
	return lazyGazetteClient
}

func WriteService() *gazette.WriteService {
	if lazyWriteService == nil {
		lazyWriteService = gazette.NewWriteService(GazetteClient())
		lazyWriteService.Start()
		ShutdownHooks = append(ShutdownHooks, lazyWriteService.Stop)
	}
	return lazyWriteService
}

// loadHints loads FSMHints given a locator, which can take the form of a simple path
// to a file on disk, or an "etcd:///path/to/key".
func LoadHints(locator string) recoverylog.FSMHints {
	var u, err = url.Parse(locator)
	switch {
	case err != nil:
		log.WithField("err", err).Fatal("failed to parse URL")
	case u.Host != "":
		log.WithField("host", u.Host).Fatal("url.Host should be empty (use `etcd:///path/to/key` syntax)")
	case u.Scheme != "" && u.Scheme != "etcd":
		log.WithField("scheme", u.Scheme).Fatal("url.Scheme must be empty or `etcd://`")
	case u.RawQuery != "":
		log.WithField("query", u.RawQuery).Fatal("url.Query must be empty")
	}

	var content []byte

	if u.Scheme == "etcd" {
		var r, err = etcd.NewKeysAPI(EtcdClient()).Get(context.Background(), u.Path, nil)
		if err != nil {
			log.WithField("err", err).Fatal("failed to read hints from Etcd")
		}
		content = []byte(r.Node.Value)
	} else if content, err = ioutil.ReadFile(u.Path); err != nil {
		log.WithFields(log.Fields{"err": err, "path": u.Path}).Fatal("failed to read hints file")
	}

	var hints recoverylog.FSMHints
	if err = json.Unmarshal(content, &hints); err != nil {
		log.WithFields(log.Fields{"err": err, "hints": string(content)}).Fatal("failed to unmarshal hints")
	}
	return hints
}

var (
	lazyConsumerPlugin consumer.Consumer
	lazyGazetteClient  *gazette.Client
	lazyWriteService   *gazette.WriteService
)
