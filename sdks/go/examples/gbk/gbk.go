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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/stringx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"math/rand"
	"time"
)

func init() {
	beam.RegisterFunction(createPair)
}

func createPair(ctx context.Context, element string, emit func(string, string)) {
	keys := []string{"A", "B", "C"}
	emit(keys[rand.Intn(3)], element)
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p, s := beam.NewPipelineWithRoot()

	project := gcpopts.GetProject(ctx)
	topic := "riteshghorse-wordcap"
	col := pubsubio.Read(s, project, topic, &pubsubio.ReadOptions{})
	windowed := beam.WindowInto(s, window.NewFixedWindows(time.Second*30), col)

	str := beam.ParDo(s, stringx.FromBytes, windowed)
	keyed := beam.ParDo(s, createPair, str)
	debug.Print(s, keyed)
	transformed := beam.GroupByKey(s, keyed)

	debug.Print(s, transformed)
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
