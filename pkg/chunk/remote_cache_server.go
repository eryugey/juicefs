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
	"fmt"
	"io"
	"time"

	pb "github.com/juicedata/juicefs/pkg/rpc/remote_cache"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newRemoteCacheServer(srv pb.RemoteCacheServer, opts ...grpc.ServerOption) *grpc.Server {
	grpcServer := grpc.NewServer(append([]grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     time.Hour,
			MaxConnectionAge:      8 * time.Hour,
			MaxConnectionAgeGrace: 5 * time.Minute,
		}),
	}, opts...)...)

	pb.RegisterRemoteCacheServer(grpcServer, srv)
	return grpcServer
}

func (r *remoteCache) regMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	if r.config.CacheGroupNoShare {
		return
	}
	reg.MustRegister(r.cacheServerHits)
	reg.MustRegister(r.cacheServerHitBytes)
	reg.MustRegister(r.cacheServerMiss)
	reg.MustRegister(r.cacheServerMissBytes)
	reg.MustRegister(r.cacheServerBacksource)
	reg.MustRegister(r.cacheServerBacksourceBytes)
	reg.MustRegister(r.cacheServerCaches)
	reg.MustRegister(r.cacheServerCacheBytes)
	reg.MustRegister(r.cacheServerRemoves)
	reg.MustRegister(r.cacheServerRemoveBytes)
	reg.MustRegister(r.cacheServerUploadHist)
	reg.MustRegister(r.cacheServerDownloadHist)
}

func (r *remoteCache) initMetrics() {
	if r.config.CacheGroupNoShare {
		return
	}

	r.cacheServerHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_hits",
		Help: "load request cache hit count",
	})
	r.cacheServerHitBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_hit_bytes",
		Help: "load request cache hit bytes",
	})
	r.cacheServerMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_miss",
		Help: "load request cache miss count",
	})
	r.cacheServerMissBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_miss_bytes",
		Help: "load request cache miss bytes",
	})
	r.cacheServerBacksource = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_backsource",
		Help: "load request back source count",
	})
	r.cacheServerBacksourceBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_backsource_bytes",
		Help: "load request back source bytes",
	})
	r.cacheServerCaches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_caches",
		Help: "cache request count",
	})
	r.cacheServerCacheBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_cache_bytes",
		Help: "cache request bytes",
	})
	r.cacheServerRemoves = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_removes",
		Help: "remove request count",
	})
	r.cacheServerRemoveBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheserver_remove_bytes",
		Help: "remove request bytes",
	})
	r.cacheServerUploadHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cacheserver_upload_hist_seconds",
		Help:    "upload cache to peer latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.00001, 2, 20),
	})
	r.cacheServerDownloadHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cacheserver_download_hist_seconds",
		Help:    "download cache from peer latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.00001, 2, 20),
	})
}

func (r *remoteCache) Load(req *pb.LoadRequest, stream pb.RemoteCache_LoadServer) error {
	var rd ReadCloser
	backsource := false
	key := req.GetKey()
	cacheSize := parseObjOrigSize(key)

	if cacheSize == 0 || cacheSize > r.config.BlockSize {
		msg := fmt.Sprintf("Remote cache server Load %s: invalid size: %v", key, cacheSize)
		logger.Warn(msg)
		return status.Error(codes.InvalidArgument, msg)
	}

	rd, err := r.bcache.load(key)
	if err != nil {
		if !r.config.CacheGroupBacksource {
			r.cacheServerMiss.Add(1)
			r.cacheServerMissBytes.Add(float64(cacheSize))
			logger.Debugf("Remote cache server Load %s: NotFound", key)
			return status.Errorf(codes.NotFound, "cache %s not found", key)
		}

		// Download from source for client
		logger.Debugf("Remote cache server Load %s: local not found, back sourcing", key)
		page := NewOffPage(cacheSize)
		defer page.Release()
		if err := r.store.load(key, page, true, false); err != nil {
			msg := fmt.Sprintf("Remote cache server Load %s: store.load: %v", key, err)
			logger.Warn(msg)
			return status.Errorf(codes.Unknown, msg)
		}
		rd = NewPageReader(page)
		r.cacheServerBacksource.Add(1)
		r.cacheServerBacksourceBytes.Add(float64(cacheSize))
		backsource = true
	}
	defer rd.Close()

	// Inform client cache found and its size
	logger.Debugf("Remote cache server Load %s: CacheHit", key)
	res := &pb.LoadResponse{
		Payload: &pb.LoadResponse_Info{
			Info: &pb.CacheInfo{
				Key: key,
				Len: int64(cacheSize),
			},
		},
	}
	if err := stream.Send(res); err != nil {
		msg := fmt.Sprintf("Remote cache server Load %s: send: %v", key, err)
		logger.Warn(msg)
		return status.Error(codes.Unknown, msg)
	}

	if !backsource {
		r.cacheServerHits.Add(1)
		r.cacheServerHitBytes.Add(float64(cacheSize))
	}

	var uploaded int
	bsize := cacheSize
	if bsize > bufferSize {
		bsize = bufferSize
	}
	ctx := stream.Context()
	page := allocPage(bsize)
	defer freePage(page)
	if r.upLimit != nil {
		r.upLimit.Wait(int64(cacheSize))
	}
	start := time.Now()
	for uploaded < cacheSize {
		if err := ctx.Err(); err != nil {
			msg := fmt.Sprintf("Remote cache server Load %s: context: %v", key, err)
			logger.Warn(msg)
			return status.Error(codes.Unknown, msg)
		}
		n, err := rd.ReadAt(page.Data, int64(uploaded))
		if err != nil && err != io.EOF {
			msg := fmt.Sprintf("Remote cache server Load %s: read: %v", key, err)
			logger.Warn(msg)
			return status.Error(codes.Unknown, msg)
		}
		if n == 0 {
			msg := fmt.Sprintf("Remote cache server Load %s: unexpected EOF", key)
			logger.Warn(msg)
			return status.Error(codes.OutOfRange, msg)
		}

		res := &pb.LoadResponse{
			Payload: &pb.LoadResponse_Data{
				Data: page.Data[:n],
			},
		}
		err = stream.Send(res)
		if err != nil {
			msg := fmt.Sprintf("Remote cache server Load %s: send: %v", key, err)
			logger.Warn(msg)
			return status.Error(codes.Unknown, msg)
		}
		uploaded += n
	}
	r.cacheServerUploadHist.Observe(time.Since(start).Seconds())
	return nil
}

func (r *remoteCache) Cache(stream pb.RemoteCache_CacheServer) error {
	ctx := stream.Context()

	// First recv gets CacheInfo, which contains key and len
	req, err := stream.Recv()
	if err != nil {
		msg := fmt.Sprintf("Remote cache server Cache: recv: %v", err)
		logger.Warn(msg)
		return status.Error(codes.Unknown, msg)
	}

	cacheInfo := req.GetInfo()
	if cacheInfo == nil {
		msg := "Remote cache server Cache: request not cache info"
		logger.Warn(msg)
		return status.Error(codes.InvalidArgument, msg)
	}
	key := cacheInfo.GetKey()
	cacheSize := cacheInfo.GetLen()
	logger.Debugf("Remote cache server Cache %s", key)

	r.runningCacheLock.Lock()
	if _, ok := r.runningCache[key]; ok {
		r.runningCacheLock.Unlock()
		logger.Debugf("Remote cache server Cache %s: caching in progress", key)
		res := &pb.CacheResponse{
			Code:    pb.Code_InProgress,
			Message: "chunk caching in progress",
		}
		if err := stream.Send(res); err != nil {
			msg := fmt.Sprintf("Remote cache server Cache %s: send: %v", key, err)
			logger.Warn(msg)
			return status.Error(codes.Unknown, msg)
		}
		return nil
	} else {
		r.runningCache[key] = struct{}{}
		r.runningCacheLock.Unlock()
		go func() {
			r.runningCacheLock.Lock()
			delete(r.runningCache, key)
			r.runningCacheLock.Unlock()
		}()
	}

	// Check if key is already cached
	rd, err := r.bcache.load(key)
	if err == nil {
		_ = rd.Close()
		logger.Debugf("Remote cache server Cache %s: already cached", key)
		res := &pb.CacheResponse{
			Code:    pb.Code_AlreadyExists,
			Message: "chunk already cached",
		}
		if err := stream.Send(res); err != nil {
			msg := fmt.Sprintf("Remote cache server Cache %s: send: %v", key, err)
			logger.Warn(msg)
			return status.Error(codes.Unknown, msg)
		}
		r.cacheServerCaches.Add(1)
		r.cacheServerCacheBytes.Add(float64(cacheSize))
		return nil
	}

	// Inform client to send data
	res := &pb.CacheResponse{
		Code:    pb.Code_Continue,
		Message: "Continue cache chunk operation",
	}
	if err := stream.Send(res); err != nil {
		msg := fmt.Sprintf("Remote cache server Cache %s: send: %v", key, err)
		logger.Warn(msg)
		return status.Error(codes.Unknown, msg)
	}

	// Receive data from peer
	var got int64
	page := allocPage(int(cacheSize))
	defer freePage(page)
	if r.downLimit != nil {
		r.downLimit.Wait(cacheSize)
	}
	start := time.Now()
	for got < cacheSize {
		if err := ctx.Err(); err != nil {
			msg := fmt.Sprintf("Remote cache server Cache %s: context: %v", key, err)
			logger.Warn(msg)
			return status.Error(codes.Unknown, msg)
		}

		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				msg := fmt.Sprintf("Remote cache server Cache %s: unexpected EOF", key)
				logger.Warn(msg)
				return status.Error(codes.OutOfRange, msg)
			}
			msg := fmt.Sprintf("Remote cache server Cache %s: recv: %v", key, err)
			logger.Warn(msg)
			return status.Error(codes.Unknown, msg)
		}

		data := req.GetData()
		size := len(data)
		if got+int64(size) > cacheSize {
			msg := fmt.Sprintf("Remote cache server Cache %s: data too large (%v+%v) > %v", key, got, size, cacheSize)
			logger.Warn(msg)
			return status.Error(codes.OutOfRange, msg)
		}
		n := copy(page.Data[got:], data)
		if n != size {
			msg := fmt.Sprintf("Remote cache server Cache %s: copy expect %v got %v", key, size, n)
			logger.Warn(msg)
			return status.Error(codes.Internal, msg)
		}
		got += int64(size)
	}
	r.cacheServerDownloadHist.Observe(time.Since(start).Seconds())

	if got < cacheSize {
		msg := fmt.Sprintf("Remote cache server Cache %s: not enough data %v < %v", key, got, cacheSize)
		logger.Warn(msg)
		return status.Error(codes.DataLoss, msg)
	}

	// Cache it to local disk store in background
	page.Acquire()
	go func(){
		r.bcache.cache(key, page, true)
		page.Release()
	}()

	return nil
}

func (r *remoteCache) Remove(_ctx context.Context, req *pb.RemoveRequest) (*emptypb.Empty, error) {
	key := req.GetKey()
	logger.Debugf("Remote cache server Remove %s", key)
	r.bcache.remove(key)
	r.cacheServerRemoves.Add(1)
	r.cacheServerRemoveBytes.Add(float64(parseObjOrigSize(key)))
	return new(emptypb.Empty), nil
}
