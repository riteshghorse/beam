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

// Package timer provides structs for reading and writing timers.
package timers

import (
	"fmt"
	"reflect"
	"time"
)

var (
	ProviderType = reflect.TypeOf((*Provider)(nil)).Elem()
)

type TimeDomainEnum int32

const (
	TimeDomainUnspecified    TimeDomainEnum = 0
	TimeDomainEventTime      TimeDomainEnum = 1
	TimeDomainProcessingTime TimeDomainEnum = 2
)

type TimerMap struct {
	Key                          string
	Tag                          string
	Clear                        bool
	FireTimestamp, HoldTimestamp int64
}

type Provider interface {
	Set(t TimerMap)
}

type PipelineTimer interface {
	TimerKey() string
	TimerDomain() TimeDomainEnum
}

type EventTimeTimer struct {
	// need to export them otherwise the key comes out empty in execution?
	Key  string
	Kind TimeDomainEnum
}

func (t *EventTimeTimer) Set(p Provider, FiringTimestamp time.Time) {
	fmt.Print("setting timer at event time")
	p.Set(TimerMap{Key: t.Key, FireTimestamp: FiringTimestamp.UnixMilli()})
}

func (e EventTimeTimer) TimerKey() string {
	return e.Key
}

func (e EventTimeTimer) TimerDomain() TimeDomainEnum {
	return e.Kind
}

// type ProcessingTimeTimer struct {
// 	timerSpec
// }

// func (e ProcessingTimeTimer) TimerKey() string {
// 	return e.timerSpec.(timerInfo).key
// }

// func (e ProcessingTimeTimer) TimerDomain() TimeDomainEnum {
// 	return e.timerSpec.(timerInfo).kind
// }

func MakeEventTimeTimer(Key string) EventTimeTimer {
	return EventTimeTimer{Key: Key, Kind: TimeDomainEventTime}
}

// func MakeProcessingTimeTimer(Key string) ProcessingTimeTimer {
// 	return ProcessingTimeTimer{timerInfo{key: Key, kind: TimeDomainProcessingTime}}
// }
