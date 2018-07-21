package internal

import (
	"fmt"
	"strings"

	etcd "github.com/coreos/etcd/client"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
)

func EtcdClient() etcd.Client {
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

func Etcd3Client() *clientv3.Client {
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

func CloudFS() cloudstore.FileSystem {
	if lazyCFS == nil {
		var cfs, err = cloudstore.NewFileSystem(nil, viper.GetString("cloud.fs.url"))
		if err != nil {
			log.WithField("err", err).Fatal("cannot initialize cloud filesystem")
		}
		lazyCFS = cfs
	}
	return lazyCFS
}

func UserConfirms(message string) {
	if DefaultYes {
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

	DefaultYes    bool
	ShutdownHooks []func()
)
