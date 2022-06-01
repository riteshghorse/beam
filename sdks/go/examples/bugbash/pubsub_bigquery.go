package main

import (
	"context"
	"flag"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/stringx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"reflect"
	"strings"
	"time"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*Caps)(nil)))
}

type Caps struct{}

func (c *Caps) ProcessElement(ctx context.Context, element string, emit func(string)) {
	emit(strings.ToUpper(element))
}

func main() {
	flag.Parse()
	beam.Init()

	ctx := context.Background()
	project := gcpopts.GetProject(ctx)

	p, s := beam.NewPipelineWithRoot()
	topic := "riteshghorse-bash"
	col := pubsubio.Read(s, project, topic, &pubsubio.ReadOptions{})
	//wscol := beam.WindowInto(s, window.NewGlobalWindows(), col, beam.Trigger(trigger.AfterCount(int32(100))))
	//wscol := beam.WindowInto(s, window.NewFixedWindows(time.Second*10), col, beam.Trigger(trigger.AfterProcessingTime().PlusDelay(time.Second*20)))
	wscol := beam.WindowInto(s, window.NewFixedWindows(time.Second*10), col, beam.Trigger(trigger.AfterCount(int32(1))))

	scol := beam.ParDo(s, stringx.FromBytes, wscol)

	ucol := beam.ParDo(s, &Caps{}, scol)

	//debug.Print(s, wscol)
	//gcol := beam.GroupByKey(s, wscol)
	//debug.Print(s, wscol)
	//ocol := beam.ParDo(s, stringx.ToBytes, ucol)

	textio.Write(s, "gs://clouddfe-riteshghorse/output.txt", ucol)
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
