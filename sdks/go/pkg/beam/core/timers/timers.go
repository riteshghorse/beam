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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

var (
	ProviderType = reflect.TypeOf((*Provider)(nil)).Elem()
)

type timeDomainEnum int32

const (
	timeDomainUnspecified    timeDomainEnum = 0
	timeDomainEventTime      timeDomainEnum = 1
	timeDomainProcessingTime timeDomainEnum = 2
)

type TimerMap struct {
	Key                          string
	Tag                          string
	Clear                        bool
	FireTimestamp, HoldTimestamp mtime.Time
}

type Provider interface {
	Set(t TimerMap)
}

type PipelineTimer interface {
	TimerKey() string
	TimerDomain() timeDomainEnum
}

type EventTimeTimer struct {
	// need to export them otherwise the key comes out empty in execution?
	Key  string
	Kind timeDomainEnum
}

func (t *EventTimeTimer) Set(p Provider, FiringTimestamp mtime.Time) {
	p.Set(TimerMap{Key: t.Key, FireTimestamp: FiringTimestamp})
}

func (t *EventTimeTimer) SetWithTag(p Provider, tag string, FiringTimestamp mtime.Time) {
	p.Set(TimerMap{Key: t.Key, Tag: tag, FireTimestamp: FiringTimestamp})
}

func (e EventTimeTimer) TimerKey() string {
	return e.Key
}

func (e EventTimeTimer) TimerDomain() timeDomainEnum {
	return e.Kind
}

func MakeEventTimeTimer(Key string) EventTimeTimer {
	return EventTimeTimer{Key: Key, Kind: timeDomainEventTime}
}
