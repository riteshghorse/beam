package inference

import (
	"flag"
	"log"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/samza"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/spark"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/ml/inference"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*TestRow)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*Row)(nil)).Elem())
}

var expansionAddr string // Populate with expansion address labelled "python_transform"
func checkFlags(t *testing.T) {
	if expansionAddr == "" {
		t.Skip("No Python Expansion Service address provided.")
	}
}

type TestRow struct {
	Example   []int64 `beam:"example"`
	Inference int32   `beam:"inference"`
}

type Row struct {
	Values []int64
}

func TestRunInference(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	// ctx := context.Background()
	row0 := TestRow{Example: []int64{0, 0}, Inference: 0}
	row1 := TestRow{Example: []int64{1, 1}, Inference: 1}

	p, s := beam.NewPipelineWithRoot()

	beam.Impulse(s)
	inputRow := []TestRow{
		{
			Example:   []int64{0, 0},
			Inference: 0,
		},
		{
			Example:   []int64{1, 1},
			Inference: 0,
		},
	}
	input := beam.CreateList(s, inputRow)

	kwargs := inference.KwargsStruct{
		ModelURI: "/tmp/staged/sklearn_model",
	}
	outCol := inference.RunInference(s, "apache_beam.ml.inference.sklearn_inference.SklearnModelHandlerNumpy", input, reflect.TypeOf((*TestRow)(nil)).Elem(), inference.WithKwarg(kwargs), inference.WithExpansionAddr(expansionAddr))

	passert.Equals(s, outCol, row0, row1)

	ptest.RunAndValidate(t, p)
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	services := integration.NewExpansionServices()
	defer func() { services.Shutdown() }()
	addr, err := services.GetAddr("python_transform")
	if err != nil {
		log.Printf("skipping missing expansion service: %v", err)
	} else {
		expansionAddr = addr
	}

	ptest.MainRet(m)
}
