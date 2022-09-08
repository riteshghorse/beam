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

package exec

// func equalTimers(a, b typex.TimerMap) bool {
// 	return a.Key == b.Key && a.Tag == b.Tag && a.FireTimestamp == b.FireTimestamp
// }
// func TestEncodeTimer(t *testing.T) {
// 	wec := MakeWindowEncoder(window.NewGlobalWindows().Coder())
// 	win, err := EncodeWindow(wec, window.SingleGlobalWindow[0])
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	tm := typex.TimerMap{
// 		Key:           "Basic",
// 		Tag:           "first",
// 		Windows:       win,
// 		Clear:         false,
// 		FireTimestamp: time.Now().UnixMilli(),
// 	}
// 	var buf bytes.Buffer
// 	err = EncodeTimer(tm, &buf)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	got, err := coder.DecodeTimer(&buf)
// 	if err != nil {
// 		t.Fatalf("failed to decode timer from buffer %v, got %v", &buf, err)
// 	}
// 	if want := tm; !equalTimers(got, want) {
// 		t.Errorf("got timer %v, want %v", got, want)
// 	}
// }
