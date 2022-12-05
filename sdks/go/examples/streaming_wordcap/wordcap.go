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

// streaming_wordcap is a toy streaming pipeline that uses PubSub. It
// does the following:
//    (1) create a topic and publish a few messages to it
//    (2) start a streaming pipeline that converts the messages to
//        upper case and logs the result.
//
// NOTE: it only runs on Dataflow and must be manually cancelled.
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/pubsubx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

var (
	input = flag.String("input", os.ExpandEnv("$USER-wordcap"), "Pubsub input topic.")
)

var (
	data = []string{
		"foo",
		"bar",
		"baz",
	}
)

func init() {
	register.DoFn6x3[context.Context, beam.EventTime, state.Provider, timers.Provider, int64, string, int64, string, error](&Stateful{})
}

type Stateful struct {
	Val  state.Value[int]
	Fire timers.EventTimeTimer
}

func (s *Stateful) ProcessElement(ctx context.Context, ts beam.EventTime, p state.Provider, tp timers.Provider, key int64, word string) (int64, string, error) {
	log.Info(ctx, "stateful dofn invoked")
	// Get the Value stored in our state
	Val, ok, err := s.Val.Read(p)
	if err != nil {
		return key, word, err
	}
	if !ok {
		s.Val.Write(p, 1)
	} else {
		s.Val.Write(p, Val+1)
	}

	if Val > 10000 {
		// Example of clearing and starting again with an empty bag
		s.Val.Clear(p)
	}

	s.Fire.Set(tp, ts.Add(time.Second*10))

	return key, word, nil
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	project := gcpopts.GetProject(ctx)

	log.Infof(ctx, "Publishing %v messages to: %v", len(data), *input)

	defer pubsubx.CleanupTopic(ctx, project, *input)
	sub, err := pubsubx.Publish(ctx, project, *input, data...)
	if err != nil {
		log.Fatal(ctx, err)
	}

	log.Infof(ctx, "Running streaming wordcap with subscription: %v", sub.ID())

	p := beam.NewPipeline()
	s := p.Root()

	col := pubsubio.Read(s, project, *input, &pubsubio.ReadOptions{Subscription: sub.ID()})
	col = beam.WindowInto(s, window.NewFixedWindows(60*time.Second), col)
	str := beam.ParDo(s, func(b []byte) string {
		return (string)(b)
	}, col)

	keyed := beam.ParDo(s, func(s string) (int64, string) {
		return int64(1), s
	}, str)

	timed := beam.ParDo(s, &Stateful{Val: state.MakeValueState[int]("key1"), Fire: timers.MakeEventTimeTimer("key2")}, keyed)
	debug.Print(s, timed)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
