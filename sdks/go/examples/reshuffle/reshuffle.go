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

package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/stringx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"reflect"
	"time"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*MultiplyByTen)(nil)))
}

func init() {
	beam.RegisterType(reflect.TypeOf((*Caps)(nil)))
}

type MultiplyByTen struct{}

func (m *MultiplyByTen) ProcessElement(ctx context.Context, element string, emit func(string)) {
	for i := 0; i < 10; i++ {
		emit(element)
	}

}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p, s := beam.NewPipelineWithRoot()

	project := "pubsub-public-data"
	topic := "taxirides-realtime"
	col := pubsubio.Read(s, project, topic, &pubsubio.ReadOptions{})
	wscol := beam.WindowInto(s, window.NewFixedWindows(time.Minute), col, beam.Trigger(trigger.Default()))

	scol := beam.ParDo(s, stringx.FromBytes, wscol)

	tcol := beam.Reshuffle(s, scol)
	rcol := beam.ParDo(s, &MultiplyByTen{}, tcol)

	project = gcpopts.GetProject(ctx)
	output := "riteshghorse-taxirides"

	ocol := beam.ParDo(s, stringx.ToBytes, rcol)
	pubsubio.Write(s, project, output, ocol)
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
