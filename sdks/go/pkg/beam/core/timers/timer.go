// Licensed to the Apache SofiringTimestampware Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, sofiringTimestampware
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
	Type         = reflect.TypeOf((*TimerInfo)(nil)).Elem() // TODO(riteshghorse): type in typex package?
)

type TimeDomain_Enum int32

const (
	TimeDomain_Unspecified    TimeDomain_Enum = 0
	TimeDomain_EventTime      TimeDomain_Enum = 1
	TimeDomain_ProcessingTime TimeDomain_Enum = 2
)

type PipelineTimer interface {
	TimerKey() string
	TimerDomain() TimeDomain_Enum
}

// required struct format for timer coder
type timerMData struct {
	Key                          []byte // elm type.
	Tag                          string
	Windows                      []byte // []typex.Window
	Clear                        bool
	FireTimestamp, HoldTimestamp int64
	Span                         int
}

type TimerMetadata struct {
	key, tag                     string
	clear                        bool
	firetimestamp, holdtimestamp int64
}

func (t *TimerMetadata) TimerKey() string {
	return t.key
}

type Provider interface {
	Set(t TimerMetadata)
}

type TimerInfo struct {
	key  string
	kind TimeDomain_Enum
}

type EventTime struct {
	*TimerInfo
}

type ProcessingTime struct {
	TimerInfo
}

func MakeEventTimeTimer(key string) *TimerInfo {
	return &TimerInfo{
		key:  key,
		kind: TimeDomain_EventTime,
	}
}

func MakeProcessingTimeTimer(key string) *TimerInfo {
	return &TimerInfo{
		key:  key,
		kind: TimeDomain_ProcessingTime,
	}
}

func (t *TimerInfo) TimerKey() string {
	return t.key
}

func (t *TimerInfo) TimerDomain() TimeDomain_Enum {
	return TimeDomain_EventTime
}

func (t *TimerInfo) Set(p Provider, firingTimestamp time.Time) {
	p.Set(TimerMetadata{
		key:           t.key,
		firetimestamp: firingTimestamp.Unix(),
	})
}

func (t *TimerInfo) SetWithTag(p Provider, tag string, firingTimestamp time.Time) {
	p.Set(TimerMetadata{
		key:           t.key,
		tag:           tag,
		firetimestamp: firingTimestamp.Unix(),
	})
}

func (t *TimerInfo) SetWithOutputTimestamp(p Provider, firingTimestamp, outputTimestamp time.Time) {
	p.Set(TimerMetadata{
		key:           t.key,
		firetimestamp: firingTimestamp.Unix(),
		holdtimestamp: outputTimestamp.Unix(),
	})
}

func (t *TimerInfo) SetWithTagAndOutputTimestamp(p Provider, tag string, firingTimestamp, outputTimestamp time.Time) {
	p.Set(TimerMetadata{
		key:           t.key,
		tag:           tag,
		firetimestamp: firingTimestamp.Unix(),
		holdtimestamp: outputTimestamp.Unix(),
	})
}

func (t *TimerInfo) Clear(p Provider) {
	p.Set(TimerMetadata{
		key:   t.key,
		clear: true,
	})
}

func (t *TimerInfo) ClearTag(p Provider, tag string) {
	p.Set(TimerMetadata{
		key:   t.key,
		tag:   tag,
		clear: true,
	})
}
