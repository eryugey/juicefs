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

import "testing"

func Test_genRemoteCacheKey(t *testing.T) {
	if genRemoteCacheKey("testkey", 0) != "0:testkey" {
		t.Fatal("(testkey, 0) -> 0:testkey failed")
	}
	if genRemoteCacheKey("testkey", 100) != "100:testkey" {
		t.Fatal("(testkey, 100) -> 100:testkey failed")
	}
}

func Test_parseRemoteCacheKey(t *testing.T) {
	tcs := []struct {
		rkey    string
		replica int
		key     string
		valid   bool
	}{
		{
			rkey:    "1:testkey",
			replica: 1,
			key:     "testkey",
			valid:   true,
		},
		{
			rkey:    "-1:chunks/1/2/3/4",
			replica: -1,
			key:     "chunks/1/2/3/4",
			valid:   true,
		},
		{
			rkey:    "2:testkey:with:colon",
			replica: 2,
			key:     "testkey:with:colon",
			valid:   true,
		},
		{
			rkey:    "t:invalidreplica",
			replica: 0,
			key:     "",
			valid:   false,
		},
		{
			rkey:    "nocoloninkey",
			replica: 0,
			key:     "",
			valid:   false,
		},
	}

	for _, tc := range tcs {
		key, n, err := parseRemoteCacheKey(tc.rkey)
		if tc.valid && err != nil {
			t.Fatalf("%s: got error %v", tc.rkey, err)
		} else if tc.valid {
			if n != tc.replica {
				t.Fatalf("%s: wrong replica number, %d != %d", tc.rkey, n, tc.replica)
			}
			if key != tc.key {
				t.Fatalf("%s: wrong key, %s != %s", tc.rkey, key, tc.key)
			}
		}

		if !tc.valid && err == nil {
			t.Fatalf("%s: got no error", tc.rkey)
		}
	}
}
