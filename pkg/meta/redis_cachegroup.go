//go:build !noredis
// +build !noredis

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

package meta

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

func (m *redisMeta) cacheGroupKey(group string) string {
	return m.prefix + "cacheGroup" + group
}

func (m *redisMeta) cacheGroupPeerKey(group, addr string) string {
	return m.prefix + "cacheGroup" + group + addr
}

// Get all peer addrs in the given cache group
func (m *redisMeta) GetCacheGroupPeers(group string) ([]string, error) {
	cgKey := m.cacheGroupKey(group)
	members, err := m.rdb.SMembers(Background, cgKey).Result()
	if err != nil {
		return nil, fmt.Errorf("SMembers %s: %v", group, err)
	}
	return members, nil
}

// Add addr to the given cache group
func (m *redisMeta) AddCacheGroupPeer(group, addr string, expire time.Duration) error {
	var ctx = Background
	cgKey := m.cacheGroupKey(group)
	peerKey := m.cacheGroupPeerKey(group, addr)

	_, err := m.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.SAdd(ctx, cgKey, addr)
		pipe.Set(ctx, peerKey, addr, expire)
		return nil
	})
	return err
}

// Remove addr from the given cache group, no-op if addr isn't in the group
func (m *redisMeta) RemoveCacheGroupPeer(group, addr string) error {
	cgKey := m.cacheGroupKey(group)
	_, err := m.rdb.SRem(Background, cgKey, addr).Result()
	if err != nil {
		return fmt.Errorf("SRem %s with %s: %v", group, addr, err)
	}
	return nil
}

func (m *redisMeta) RefreshCacheGroupPeer(group, addr string, expire time.Duration) error {
	peerKey := m.cacheGroupPeerKey(group, addr)
	set, err := m.rdb.Expire(Background, peerKey, expire).Result()
	if err != nil {
		return err
	}
	if !set {
		logger.Warnf("RefreshCacheGroup %s Peer %s not set", group, addr)
	}
	return nil
}

func (m *redisMeta) GcCacheGroupPeers(group string, peers []string) []string {
	cgKey := m.cacheGroupKey(group)
	deleted := []string{}
	for _, peer := range peers {
		peerKey := m.cacheGroupPeerKey(group, peer)
		err := m.rdb.Watch(Background, func(tx *redis.Tx) error {
			_, err := tx.Get(Background, peerKey).Result()
			// peer doesn't expire, still valid
			if err == nil {
				return nil
			}
			if err != nil && err != redis.Nil {
				return err
			}

			// peer expired, try remove it from cache group set
			if _, err := m.rdb.SRem(Background, cgKey, peer).Result(); err != nil {
				return err
			}
			deleted = append(deleted, peer)
			return nil
		}, cgKey, peerKey)
		if err != nil {
			logger.Warnf("GcCacheGroupPeers %s: %v", peer, err)
		}
	}
	return deleted
}
