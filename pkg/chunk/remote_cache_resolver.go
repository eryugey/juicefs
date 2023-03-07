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
	"reflect"

	"google.golang.org/grpc/resolver"
)

const (
	// remote cache scheme used by grpc resolver
	remoteCacheScheme = "rcache"
)

var (
	// remote cache virtual target, real target will be returned by customized grpc resolver
	remoteCacheVirtualTarget = remoteCacheScheme + "://localhost"
)

type observer interface {
	// onNotify is called when new event triggered
	onNotify()
}

type remoteCacheResolverUser interface {
	// Get latest resolved addresses
	getResolvedAddrs() ([]resolver.Address, error)

	// Register allows an observer to register itself to observe events.
	register(observer)

	// Deregister deletes observer from watcher list
	deregister(observer)

	// Notify send events to registered observers
	notify()
}

type remoteCacheResolver struct {
	addrs []resolver.Address
	cc    resolver.ClientConn
	user  remoteCacheResolverUser
}

func registerRemoteCacheResolver(user remoteCacheResolverUser) {
	resolver.Register(&remoteCacheResolver{user: user})
}

// Scheme returns the resolver scheme.
func (r *remoteCacheResolver) Scheme() string {
	return remoteCacheScheme
}

// Build creates a new resolver for the given target.
func (r *remoteCacheResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.cc = cc
	r.user.register(r)
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

// ResolveNow will be called by gRPC to try to resolve the target name again,
// gRPC will trigger resolveNow everytime SubConn connect fail.
func (r *remoteCacheResolver) ResolveNow(resolver.ResolveNowOptions) {
	addrs, err := r.user.getResolvedAddrs()
	if err != nil {
		logger.Errorf("ResolveNow getResolvedAddrs: %v", err)
		return
	}

	if len(addrs) == 0 {
		logger.Warn("ResolveNow got zero address")
		return
	}
	if reflect.DeepEqual(r.addrs, addrs) {
		return
	}
	r.addrs = addrs

	if err := r.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
		logger.Warnf("ResolveNow UpdateState: %v", err)
	}
	return
}

// Close closes the resolver.
func (r *remoteCacheResolver) Close() {
	r.user.deregister(r)
}

// onNotify is triggered when resolver address is updated.
func (r *remoteCacheResolver) onNotify() {
	r.ResolveNow(resolver.ResolveNowOptions{})
}
