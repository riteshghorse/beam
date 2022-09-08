// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coder

// func equalTimers(a, b typex.TimerMap) bool {
// 	return a.FireTimestamp == b.FireTimestamp
// }
// func TestEncodeTimer(t *testing.T) {
// 	tm := typex.TimerMap{
// 		Key:           "Basic",
// 		Tag:           "",
// 		Windows:       []byte{},
// 		Clear:         false,
// 		FireTimestamp: time.Now().UnixMilli(),
// 	}
// 	var buf bytes.Buffer
// 	err := EncodeTimer(tm, &buf)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	got, err := DecodeTimer(&buf)
// 	if err != nil {
// 		t.Fatalf("failed to decode timer from buffer %v, got %v", &buf, err)
// 	}
// 	if want := tm; !equalTimers(got, want) {
// 		t.Errorf("got pane %v, want %v", got, want)
// 	}
// }
