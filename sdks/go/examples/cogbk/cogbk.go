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
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/stringx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
	"sort"
	"strings"
	"time"
)

func init() {
	beam.RegisterFunction(pairWithTs)
	beam.RegisterFunction(pairWithWindow)
	beam.RegisterFunction(formatCoGBKResults)
}

func pairWithTs(ctx context.Context, ts beam.EventTime, element string, emit func(string, string)) {
	emit(ts.String(), element)
}

func pairWithWindow(ctx context.Context, w beam.Window, ts beam.EventTime, _ string, emit func(string, string)) {
	emit(ts.String(), fmt.Sprintf("%v", w))
}

func formatCoGBKResults(key string, tsIter, wIter func(*string) bool) string {
	var s string
	var ts, w []string
	for tsIter(&s) {
		ts = append(ts, s)
	}
	for wIter(&s) {
		w = append(w, s)
	}
	// Values have no guaranteed order, sort for deterministic output.
	sort.Strings(ts)
	sort.Strings(w)
	return fmt.Sprintf("%s; %s; %s", key, strings.Join(ts, ","), strings.Join(w, ","))
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
	keyed := beam.ParDo(s, pairWithTs, str)
	anotherKeyed := beam.ParDo(s, pairWithWindow, str)
	transformed := beam.CoGroupByKey(s, keyed, anotherKeyed)

	output := beam.ParDo(s, formatCoGBKResults, transformed)
	debug.Print(s, output)
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
