package mackerelcache

import pb "github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"

type Watcher interface {
}

type watcher struct {
	clients map[string]pb.WatchServiceClient
}
