// Licensed to the Apache SoFiringTimestampware Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, soFiringTimestampware
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package timers

import (
	"fmt"
	"testing"
	"time"
)

type testProvider struct{}

func (*testProvider) Set(t TimerMap) {
	fmt.Print("setting timer")
}

func TestEventTimers(t *testing.T) {
	timers := MakeEventTimeTimer("timer")
	// if timers == nil {
	// 	t.Fatal("timers is nil")
	// }

	timers.Set(&testProvider{}, time.Now().Add(time.Second*5))
}
