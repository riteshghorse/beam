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

import (
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

// EncodeTimer encodes a typex.PaneInfo.
func EncodeTimer(tm typex.TimerMap, w io.Writer) error {
	EncodeStringUTF8(tm.Key, w)
	EncodeStringUTF8(tm.Tag, w)
	EncodeBytes(tm.Windows, w)
	EncodeBool(tm.Clear, w)
	EncodeVarInt(tm.FireTimestamp-(-9223372036854775808), w)
	EncodeVarInt(tm.HoldTimestamp-(-9223372036854775808), w)
	EncodePane(tm.PaneInfo, w)
	// log.Fatal(context.Background(), "encoding timer successfully")
	return nil
}

// DecodeTimer decodes a single byte.
func DecodeTimer(r io.Reader) (typex.Timers, error) {
	tm := typex.Timers{}
	// var err error
	if b, err := DecodeBytes(r); err != nil {
		return tm, err
	} else {
		tm.Key = b
	}

	if s, err := DecodeStringUTF8(r); err != nil {
		return tm, nil
	} else {
		tm.Tag = s
	}

	if w, err := DecodeBytes(r); err != nil {
		return tm, nil
	} else {
		tm.Windows = w
	}

	if c, err := DecodeBool(r); err != nil {
		return tm, nil
	} else {
		tm.Clear = c
	}

	if ft, err := DecodeVarInt(r); err != nil {
		return tm, nil
	} else {
		tm.FireTimestamp = ft
	}

	if ht, err := DecodeVarInt(r); err != nil {
		return tm, nil
	} else {
		tm.HoldTimestamp = ht
	}

	if pn, err := DecodePane(r); err != nil {
		return tm, nil
	} else {
		tm.PaneInfo = pn
	}
	return tm, nil
}
