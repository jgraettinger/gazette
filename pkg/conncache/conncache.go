// Package conncache implements a global, shared LRU cache of gRPC ClientConns.
package conncache

import (
	"github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func init() {
	var err error

	cache, err = lru.NewWithEvict(1024,
		func(key, value interface{}) {
			_ = value.(*grpc.ClientConn).Close()
			log.WithField("endpoint", key).Debug("conncache: dropped gRPC connection")
		})

	if err != nil {
		panic(err.Error())
	}
}

// Get fetches the current ClientConn for |endpoint| from the cache. If the cache
// misses, it dials |endpoint| with the provided DialOptions.
func Get(endpoint string, dialOpts ...grpc.DialOption) (conn *grpc.ClientConn, err error) {
	if v, ok := cache.Get(endpoint); ok {
		conn = v.(*grpc.ClientConn)
	} else if conn, err = grpc.Dial(endpoint, dialOpts...); err == nil {
		log.WithField("endpoint", endpoint).Debug("conncache: built gRPC connection")
		cache.Add(endpoint, conn)
	}
	return
}

var cache *lru.Cache
