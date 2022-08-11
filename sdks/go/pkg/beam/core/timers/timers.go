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
	"reflect"
	"time"
)

var (
	ProviderType = reflect.TypeOf((*Provider)(nil)).Elem()
)

type TimeDomain_Enum int32

const (
	TimeDomain_Unspecified    TimeDomain_Enum = 0
	TimeDomain_EventTime      TimeDomain_Enum = 1
	TimeDomain_ProcessingTime TimeDomain_Enum = 2
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

type timerSpec interface {
	Set(p Provider, FiringTimestamp time.Time)
}

type timerInfo struct {
	key  string
	kind TimeDomain_Enum
}

func (t timerInfo) Set(p Provider, FiringTimestamp time.Time) {
	p.Set(TimerMap{
		Key:           t.key,
		FireTimestamp: FiringTimestamp.Unix(),
	})
}

type PipelineTimer interface {
	TimerKey() string
	TimerDomain() TimeDomain_Enum
}

type EventTimeTimer struct {
	timerSpec
}

func (e EventTimeTimer) TimerKey() string {
	return e.timerSpec.(timerInfo).key
}

func (e EventTimeTimer) TimerDomain() TimeDomain_Enum {
	return e.timerSpec.(timerInfo).kind
}

type ProcessingTimeTimer struct {
	timerSpec
}

func (e ProcessingTimeTimer) TimerKey() string {
	return e.timerSpec.(timerInfo).key
}

func (e ProcessingTimeTimer) TimerDomain() TimeDomain_Enum {
	return e.timerSpec.(timerInfo).kind
}

func MakeEventTimeTimer(Key string) *EventTimeTimer {
	return &EventTimeTimer{timerInfo{key: Key, kind: TimeDomain_EventTime}}
}

func MakeProcessingTimeTimer(Key string) ProcessingTimeTimer {
	return ProcessingTimeTimer{timerInfo{key: Key, kind: TimeDomain_ProcessingTime}}
}
