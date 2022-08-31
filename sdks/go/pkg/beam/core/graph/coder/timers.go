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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// EncodeTimer encodes a typex.PaneInfo.
func EncodeTimer(tm typex.TimerMap, w io.Writer) error {
	// w.Write(tm.Key)

	if err := EncodeStringUTF8(tm.Key, w); err != nil {
		return errors.WithContext(err, "error encoding key")
	}
	if err := EncodeStringUTF8(tm.Tag, w); err != nil {
		return errors.WithContext(err, "error encoding tag")
	}
	// w.Write(tm.Windows)
	// if err := EncodeBytes(tm.Windows, w); err != nil {
	// 	return errors.WithContext(err, "error encoding windows")
	// }

	if err := EncodeBool(tm.Clear, w); err != nil {
		return errors.WithContext(err, "error encoding key")
	}
	if err := EncodeVarInt(tm.FireTimestamp, w); err != nil {
		return errors.WithContext(err, "error encoding key")
	}
	if err := EncodeVarInt(tm.HoldTimestamp, w); err != nil {
		return errors.WithContext(err, "error encoding key")
	}
	if err := EncodePane(tm.PaneInfo, w); err != nil {
		return errors.WithContext(err, "error encoding key")
	}
	// log.Fatal(context.Background(), "encoding timer successfully")
	return nil
}

// DecodeTimer decodes a single byte.
func DecodeTimer(r io.Reader) (typex.TimerMap, error) {
	tm := typex.TimerMap{}

	if s, err := DecodeStringUTF8(r); err != nil && err != io.EOF {
		return tm, errors.WithContext(err, "error decoding key")
	} else {
		tm.Key = s
	}
	if s, err := DecodeStringUTF8(r); err != nil && err != io.EOF {
		return tm, errors.WithContext(err, "error decoding tag")
	} else {
		tm.Tag = s
	}
	// r.Read(tm.Windows)
	// if win, err := DecodeBytes(r); err != nil && err != io.EOF {
	// 	return tm, errors.WithContext(err, "error decoding key")
	// } else {
	// 	tm.Windows = win
	// }

	if c, err := DecodeBool(r); err != nil {
		return tm, errors.WithContext(err, "error decoding clear")
	} else {
		tm.Clear = c
	}
	if ft, err := DecodeVarInt(r); err != nil {
		return tm, errors.WithContext(err, "error decoding ft")
	} else {
		tm.FireTimestamp = ft
	}
	if ht, err := DecodeVarInt(r); err != nil {
		return tm, errors.WithContext(err, "error decoding ht")
	} else {
		tm.HoldTimestamp = ht
	}
	if pn, err := DecodePane(r); err != nil {
		return tm, errors.WithContext(err, "error decoding pn")
	} else {
		tm.PaneInfo = pn
	}
	return tm, nil
}
