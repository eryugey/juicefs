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

import "fmt"

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
func (m *redisMeta) AddCacheGroupPeer(group, addr string) error {
	cgKey := m.cacheGroupKey(group)
	_, err := m.rdb.SAdd(Background, cgKey, addr).Result()
	if err != nil {
		return fmt.Errorf("SAdd %s with %s: %v", group, addr, err)
	}
	return nil
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
