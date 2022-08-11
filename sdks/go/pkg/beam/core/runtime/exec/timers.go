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

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type UserTimerAdapter interface {
	NewTimerProvider(ctx context.Context, manager TimerManager, w typex.Window, element interface{}) (timerProvider, error)
}

type userTimerAdapter struct {
	sID        StreamID
	wc         WindowEncoder
	kc         ElementEncoder
	ec         ElementDecoder
	timerCoder *coder.Coder
	c          *coder.Coder
}

func NewUserTimerAdapter(sID StreamID, c *coder.Coder, timerCoder *coder.Coder) UserTimerAdapter {
	if !coder.IsW(c) {
		panic(fmt.Sprintf("expected WV coder for user timer %v: %v", sID, c))
	}

	wc := MakeWindowEncoder(c.Window)
	var kc ElementEncoder
	var ec ElementDecoder

	if coder.IsKV(coder.SkipW(c)) {
		kc = MakeElementEncoder(coder.SkipW(c).Components[0])
		ec = MakeElementDecoder(coder.SkipW(c).Components[1])
	} else {
		ec = MakeElementDecoder(coder.SkipW(c))
	}
	return &userTimerAdapter{sID: sID, wc: wc, kc: kc, ec: ec, c: c, timerCoder: timerCoder}
}

func (u userTimerAdapter) NewTimerProvider(ctx context.Context, manager TimerManager, w typex.Window, element interface{}) (timerProvider, error) {
	elementKey, err := EncodeElement(u.kc, element.(*MainInput).Key.Elm)
	if err != nil {
		return timerProvider{}, err
	}

	win, err := EncodeWindow(u.wc, w)
	if err != nil {
		return timerProvider{}, err
	}
	tp := timerProvider{
		ctx:          ctx,
		dm:           manager,
		SID:          u.sID,
		elementKey:   elementKey,
		window:       win,
		writersByKey: make(map[string]*io.WriteCloser),
		timerCoder:   u.timerCoder,
	}

	return tp, nil
}
