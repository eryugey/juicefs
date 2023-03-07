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

	pb "github.com/juicedata/juicefs/pkg/rpc/remote_cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/credentials/insecure"
)

type remoteCacheClient interface {
	load(context.Context, *pb.LoadRequest, ...grpc.CallOption) (pb.RemoteCache_LoadClient, error)
	cache(context.Context, string, ...grpc.CallOption) (pb.RemoteCache_CacheClient, error)
	remove(context.Context, *pb.RemoveRequest, ...grpc.CallOption) error
}

type rClient struct {
	client pb.RemoteCacheClient
}

func newRemoteCacheClient(ctx context.Context, user remoteCacheResolverUser, opts ...grpc.DialOption) (remoteCacheClient, error) {
	// Register grpc resolver and balancer
	registerRemoteCacheResolver(user)
	balancer.Register(newConsistentHashingBuilder())

	conn, err := grpc.DialContext(
		ctx,
		remoteCacheVirtualTarget,
		append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(balancerServiceConfig),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}
	return &rClient{client: pb.NewRemoteCacheClient(conn)}, nil
}

func (c *rClient) load(ctx context.Context, req *pb.LoadRequest, opts ...grpc.CallOption) (pb.RemoteCache_LoadClient, error) {
	return c.client.Load(context.WithValue(ctx, contextKey, req.GetKey()), req, opts...)
}

func (c *rClient) cache(ctx context.Context, key string, opts ...grpc.CallOption) (pb.RemoteCache_CacheClient, error) {
	return c.client.Cache(context.WithValue(ctx, contextKey, key), opts...)
}

func (c *rClient) remove(ctx context.Context, req *pb.RemoveRequest, opts ...grpc.CallOption) error {
	_, err := c.client.Remove(context.WithValue(ctx, contextKey, req.GetKey()), req, opts...)
	return err
}
