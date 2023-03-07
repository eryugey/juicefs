/*
 * Copyright 2023 Alibaba Cloud, Inc. or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package chunk

import (
	"errors"
	"reflect"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"stathat.com/c/consistent"
)

type ContextKeyType string

const (
	// balancerName is the name of consistent-hashing balancer.
	balancerName = "consistent-hashing"

	// balancerServiceConfig is a service config that sets the default balancer
	// to the consistent-hashing balancer.
	balancerServiceConfig = `{"loadBalancingPolicy":"consistent-hashing"}`

	// contextKey is the key for the grpc request's context.Context which points to
	// the key to hash for the request. The value it points to must be []byte
	contextKey = ContextKeyType("chunk-key")
)

// NewConsistentHashingBuilder creates a new balancer.Builder that will create a consistent
// hashring balancer with the given config.
func newConsistentHashingBuilder() balancer.Builder {
	return base.NewBalancerBuilder(
		balancerName,
		&consistentHashingPickerBuilder{},
		base.Config{HealthCheck: true},
	)
}

type consistentHashingPickerBuilder struct{}

func (b *consistentHashingPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("consistentHashingPicker: newPicker called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	hashring := consistent.New()
	scs := make(map[string]balancer.SubConn, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		node := scInfo.Address.Addr
		hashring.Add(node)
		scs[node] = sc
	}

	return &consistentHashingPicker{
		subConns: scs,
		hashring: hashring,
	}
}

type consistentHashingPicker struct {
	subConns map[string]balancer.SubConn
	hashring *consistent.Consistent
}

func (p *consistentHashingPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	key, ok := info.Ctx.Value(contextKey).(string)
	if !ok {
		return balancer.PickResult{}, errors.New("picker can not found chunk key")
	}

	node, err := p.hashring.Get(key)
	if err != nil {
		return balancer.PickResult{}, err
	}
	logger.Debugf("key %s picks node %s among %v", key, node, reflect.ValueOf(p.subConns).MapKeys())

	return balancer.PickResult{
		SubConn: p.subConns[node],
	}, nil
}
