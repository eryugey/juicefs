/*
 * Copyright 2023 Alibaba Cloud, Inc. or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/reachable"
	pb "github.com/juicedata/juicefs/pkg/rpc/remote_cache"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juju/ratelimit"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

const (
	// Watch cache group peers change interval
	watchInterval = time.Second * 10

	// Peer keepalive interval
	peerKeepaliveInterval = time.Minute

	// Peer GC interval
	peerGcInterval = time.Minute * 5

	// How long a peer expires in db, should be larger than keep alive interval
	peerExpireTime = time.Minute * 5

	// Download/upload buffer size
	bufferSize = 1024 * 1024
)

type RemoteCache interface {
	cache(key string, p *Page) error
	remove(key string)
	load(key string) (ReadCloser, error)
	stop()
}

type remoteCache struct {
	config    *Config
	meta      meta.Meta
	store     *cachedStore
	bcache    CacheManager
	localAddr string
	upLimit   *ratelimit.Bucket
	downLimit *ratelimit.Bucket

	peers      []string
	observers  map[observer]struct{}
	grpcServer *grpc.Server
	grpcClient remoteCacheClient

	stopped chan struct{}

	cacheServerHits            prometheus.Counter
	cacheServerHitBytes        prometheus.Counter
	cacheServerMiss            prometheus.Counter
	cacheServerMissBytes       prometheus.Counter
	cacheServerBacksource      prometheus.Counter
	cacheServerBacksourceBytes prometheus.Counter
	cacheServerCaches          prometheus.Counter
	cacheServerCacheBytes      prometheus.Counter
	cacheServerRemoves         prometheus.Counter
	cacheServerRemoveBytes     prometheus.Counter
	cacheServerUploadHist      prometheus.Histogram
	cacheServerDownloadHist    prometheus.Histogram
}

func newRemoteCache(config *Config, reg prometheus.Registerer, meta meta.Meta, store *cachedStore, bcache CacheManager) (RemoteCache, error) {
	rcache := &remoteCache{
		config:    config,
		meta:      meta,
		store:     store,
		bcache:    bcache,
		observers: map[observer]struct{}{},
		stopped:   make(chan struct{}),
	}

	if !config.CacheGroupNoShare {
		// Get host external IPv4 addr
		ip, err := utils.GetIpv4()
		if err != nil {
			return nil, fmt.Errorf("Failed to get IPv4 address: %v", err)
		}

		// Get listener and random port that could be listened on
		rpcListener, port, err := utils.ListenWithRandomPort(ip)
		if err != nil {
			logger.Errorf("Listen on %v failed: %v", port, err)
			return nil, err
		}
		rcache.localAddr = fmt.Sprintf("%s:%v", ip, port)

		// First create remote cache server and serve
		rcache.grpcServer = newRemoteCacheServer(rcache)
		go func() {
			logger.Infof("Serving remote cache on %s", rcache.localAddr)
			if err := rcache.grpcServer.Serve(rpcListener); err != nil {
				logger.Errorf("Serve for remote cache: %v", err)
			}
		}()

		// Then announce our remote cache addr only if server starts successfully
		r := reachable.New(&reachable.Config{
			Address: rcache.localAddr,
			Timeout: time.Second,
		})
		if err := r.Check(); err != nil {
			logger.Errorf("Remote cache server not ready for 1s: %v", err)
			return nil, err
		} else {
			if err := rcache.addCacheGroupPeer(); err != nil {
				logger.Errorf("Add cache group failed: %v", err)
				return nil, err
			}
			_, _ = rcache.getCacheGroupPeers()
			logger.Infof("Cache group peers %v", rcache.peers)
		}

		rcache.initMetrics()
		rcache.regMetrics(reg)

		go rcache.keepAliveCacheGroupPeer()
		go rcache.gcCacheGroupPeers()
	}

	// Create client after updating cache group peers, otherwise client may
	// get empty peers.
	client, err := newRemoteCacheClient(context.Background(), rcache)
	if err != nil {
		return nil, fmt.Errorf("Failed to new remote cache client: %v", err)
	}
	rcache.grpcClient = client

	if config.CacheGroupUploadLimit > 0 {
		limit := config.CacheGroupUploadLimit
		rcache.upLimit = ratelimit.NewBucketWithRate(float64(limit), limit)
	}
	if config.CacheGroupDownloadLimit > 0 {
		limit := config.CacheGroupDownloadLimit
		rcache.downLimit = ratelimit.NewBucketWithRate(float64(limit), limit)
	}

	// Refresh peer addr list in backgroud
	go rcache.refreshCacheGroupPeers()

	return rcache, nil
}

func (r *remoteCache) load(key string) (rc ReadCloser, rerr error) {
	logger.Debugf("Loading %s from remote cache", key)
	req := &pb.LoadRequest{Key: key}
	stream, err := r.grpcClient.load(context.Background(), req)
	if err != nil {
		msg := fmt.Sprintf("Load from remote %s: %v", key, err)
		logger.Warn(msg)
		return nil, errors.New(msg)
	}

	// First response is about cache info
	res, err := stream.Recv()
	if err != nil {
		// return nil error if not found
		if status.Code(err) != codes.NotFound {
			msg := fmt.Sprintf("Load from remote %s: %v", key, err)
			logger.Warn(msg)
			return nil, errors.New(msg)
		} else {
			msg := fmt.Sprintf("Load from remote %s: NotFound", key)
			logger.Debugf(msg)
			return nil, errors.New(msg)
		}
	}
	cacheInfo := res.GetInfo()
	if cacheInfo == nil {
		msg := fmt.Sprintf("Load from remote %s: response not cache info", key)
		logger.Warn(msg)
		return nil, errors.New(msg)
	}
	cacheSize := cacheInfo.GetLen()
	if cacheSize == 0 || cacheSize > int64(r.config.BlockSize) {
		msg := fmt.Sprintf("Load from remote %s: invalid cache size %v", key, cacheSize)
		logger.Warn(msg)
		return nil, errors.New(msg)
	}

	// Load cache from remote
	logger.Tracef("Load from remote found %s, size %v", key, cacheSize)
	var got int64
	page := NewOffPage(int(cacheSize))
	defer page.Release()
	ctx := stream.Context()
	if r.downLimit != nil {
		r.downLimit.Wait(cacheSize)
	}
	for got < cacheSize {
		if err := ctx.Err(); err != nil {
			msg := fmt.Sprintf("Load from remote %s: context: %v", key, err)
			logger.Warn(msg)
			return nil, errors.New(msg)
		}

		res, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				msg := fmt.Sprintf("Load from remote %s: unexpected EOF", key)
				logger.Warn(msg)
				return nil, errors.New(msg)
			} else {
				msg := fmt.Sprintf("Load from remote %s: %v", key, err)
				logger.Warn(msg)
				return nil, errors.New(msg)
			}
		}

		data := res.GetData()
		size := len(data)
		if got+int64(size) > cacheSize {
			msg := fmt.Sprintf("Load from remote %s: data too large (%v+%v) > %v", key, got, size, cacheSize)
			logger.Warn(msg)
			return nil, errors.New(msg)
		}
		n := copy(page.Data[got:], data)
		if n != size {
			msg := fmt.Sprintf("Load from remote %s: copy expect %v got %v", key, size, n)
			logger.Warn(msg)
			return nil, errors.New(msg)
		}
		got += int64(size)
	}
	if got < cacheSize {
		msg := fmt.Sprintf("Load from remote %s: not enough data %v < %v", key, got, cacheSize)
		logger.Warn(msg)
		return nil, errors.New(msg)
	}

	// Cache it to local disk store in background
	page.Acquire()
	go func() {
		r.bcache.cache(key, page, true)
		page.Release()
	}()

	// Return cache reader from page, in case local cache is disabled
	return NewPageReader(page), nil
}

func (r *remoteCache) cache(key string, p *Page) error {
	logger.Debugf("Caching %s to remote cache", key)

	stream, err := r.grpcClient.cache(context.Background(), key)
	if err != nil {
		msg := fmt.Sprintf("Cache to remote %s: %v", key, err)
		logger.Warn(msg)
		return errors.New(msg)
	}
	ctx := stream.Context()

	// First send info about cache to server
	cacheSize := len(p.Data)
	req := &pb.CacheRequest{
		Payload: &pb.CacheRequest_Info{
			Info: &pb.CacheInfo{
				Key: key,
				Len: int64(cacheSize),
			},
		},
	}
	err = stream.Send(req)
	if err != nil {
		msg := fmt.Sprintf("Cache to remote %s: send: %v", key, err)
		logger.Warn(msg)
		return errors.New(msg)
	}

	// Then server tells us what to do next
	res, err := stream.Recv()
	if err != nil {
		msg := fmt.Sprintf("Cache to remote %s: recv: %v", key, err)
		logger.Warn(msg)
		return errors.New(msg)
	}
	code := res.GetCode()

	// Cache already exists in remote server, all done
	if code == pb.Code_AlreadyExists {
		logger.Debugf("Cache %s to remote: already exists", key)
		if err := stream.CloseSend(); err != nil {
			logger.Warnf("Cache to remote %s: sendClose: %v", key, err)
		}
		return nil
	}

	// Upload cache data to remote cache
	if code != pb.Code_Continue {
		logger.Warnf("Cache to remote %s: unexpected code %v:%s", key, code, pb.Code_name[int32(code)])
	}

	if r.upLimit != nil {
		r.upLimit.Wait(int64(cacheSize))
	}
	var uploaded int
	for uploaded < cacheSize {
		if err := ctx.Err(); err != nil {
			msg := fmt.Sprintf("Cache to remote %s: context: %v", key, err)
			logger.Warn(msg)
			return errors.New(msg)
		}

		left := cacheSize - uploaded
		if left > bufferSize {
			left = bufferSize
		}
		req := &pb.CacheRequest{
			Payload: &pb.CacheRequest_Data{
				Data: p.Data[uploaded : uploaded+left],
			},
		}
		if err := stream.Send(req); err != nil {
			msg := fmt.Sprintf("Cache to remote %s: send: %v", key, err)
			logger.Warn(msg)
			return errors.New(msg)
		}
		uploaded += left
	}
	if err := stream.CloseSend(); err != nil {
		logger.Warnf("Cache to remote %s: sendClose: %v", key, err)
	}
	return nil
}

func (r *remoteCache) remove(key string) {
	logger.Debugf("Removing %s from remote cache", key)
	req := &pb.RemoveRequest{Key: key}
	err := r.grpcClient.remove(context.Background(), req)
	if err != nil {
		logger.Warnf("Remove from remote cache %s: %v", key, err)
	}
	return
}

func (r *remoteCache) refreshCacheGroupPeers() {
	tick := time.NewTicker(watchInterval)
	for {
		select {
		case <-r.stopped:
			logger.Debug("Stop refreshing cache group peers")
			return
		case <-tick.C:
			group := r.config.CacheGroup
			changed, err := r.getCacheGroupPeers()
			if err != nil {
				logger.Warnf("refresh peers of cache group %s failed: %v", group, err)
				continue
			}
			if changed {
				logger.Infof("peers of cache group %s changed: %v", group, r.peers)
				r.notify()
			} else {
				logger.Debugf("peers of cache group %s not changed", group)
			}
		}
	}
	return
}

func (r *remoteCache) keepAliveCacheGroupPeer() {
	group := r.config.CacheGroup
	addr := r.localAddr
	tick := time.NewTicker(peerKeepaliveInterval)
	for {
		select {
		case <-r.stopped:
			logger.Debug("Stop peer keepalive")
			return
		case <-tick.C:
			if err := r.meta.RefreshCacheGroupPeer(group, addr, peerExpireTime); err != nil {
				logger.Warnf("Refresh cache group %s peer %s: %v", group, addr, err)
			}
		}
	}
}

func (r *remoteCache) gcCacheGroupPeers() {
	group := r.config.CacheGroup
	tick := time.NewTicker(peerGcInterval)
	for {
		select {
		case <-r.stopped:
			logger.Debug("Stop peer gc")
			return
		case <-tick.C:
			start := time.Now()
			dels := r.meta.GcCacheGroupPeers(group, r.peers)
			logger.Infof("GC cache group %s done in %vs: %v", group, time.Since(start).Seconds(), dels)
		}
	}
}

func (r *remoteCache) addCacheGroupPeer() error {
	return r.meta.AddCacheGroupPeer(r.config.CacheGroup, r.localAddr, peerExpireTime)
}

func (r *remoteCache) getCacheGroupPeers() (bool, error) {
	group := r.config.CacheGroup
	newPeers, err := r.meta.GetCacheGroupPeers(group)
	if err != nil {
		return false, err
	}

	// Check if there's any membership change
	if (len(newPeers) != len(r.peers)) || !reflect.DeepEqual(r.peers, newPeers) {
		r.peers = newPeers
		return true, nil
	}

	return false, nil
}

func (r *remoteCache) removeCacheGroupPeer() error {
	return r.meta.RemoveCacheGroupPeer(r.config.CacheGroup, r.localAddr)
}

func (r *remoteCache) stop() {
	logger.Info("Stop remote cache")
	close(r.stopped)

	// Server wasn't started if no sharing
	if r.config.CacheGroupNoShare {
		return
	}

	// Remove this instance from cache group
	if err := r.removeCacheGroupPeer(); err != nil {
		logger.Warnf("Remove cache group failed: %v", err)
	}

	// Shutdown grpc server gracefully
	r.grpcServer.GracefulStop()
	return
}

func (r *remoteCache) getResolvedAddrs() ([]resolver.Address, error) {
	var rAddrs = []resolver.Address{}
	var addrs = []string{}
	group := r.config.CacheGroup
	changed, err := r.getCacheGroupPeers()
	if err != nil {
		msg := fmt.Sprintf("getResolvedAddrs %s failed: %v", group, err)
		logger.Warn(msg)
		return nil, errors.New(msg)
	}
	if changed {
		logger.Infof("getResolvedAddrs %s latest peers: %v", group, r.peers)
	}

	for _, addr := range r.peers {
		start := time.Now()
		r := reachable.New(&reachable.Config{Address: addr})
		if err := r.Check(); err != nil {
			logger.Warnf("Peer address %s is unreachable", addr)
			continue
		}
		logger.Tracef("reachable check %s %v s", addr, time.Since(start).Seconds())
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			logger.Warnf("Peer address %s is invalid", addr)
			continue
		}
		rAddrs = append(rAddrs, resolver.Address{
			ServerName: host,
			Addr:       addr,
		})
		addrs = append(addrs, addr)
	}
	logger.Debugf("getResolvedAddrs %s latest addrs: %v", group, addrs)
	return rAddrs, nil
}

func (r *remoteCache) register(o observer) {
	r.observers[o] = struct{}{}
}

func (r *remoteCache) deregister(o observer) {
	delete(r.observers, o)
}

func (r *remoteCache) notify() {
	for o := range r.observers {
		o.onNotify()
	}
}
