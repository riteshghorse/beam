package inference

import (
	"flag"
	"log"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/ml/inference"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

var expansionAddr string // Populate with expansion address labelled "python_transform"
func checkFlags(t *testing.T) {
	if expansionAddr == "" {
		t.Skip("No Python Expansion Service address provided.")
	}
}

func TestRunInference(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	// ctx := context.Background()
	row0 := inference.Schema{Example: []int64{0, 0}, Inference: 0}
	row1 := inference.Schema{Example: []int64{1, 1}, Inference: 1}

	p, s := beam.NewPipelineWithRoot()

	inputRow := []inference.Schema{
		{
			Example: []int64{0, 0},
		},
		{
			Example: []int64{1, 1},
		},
	}
	input := beam.CreateList(s, inputRow)
	kwargs := map[string]string{
		"model_uri": "/tmp/staged/sklearn_model",
	}
	outCol := inference.RunInference(s, "apache_beam.ml.inference.sklearn_inference.SklearnModelHandlerNumpy", input, kwargs, inference.WithExpansionAddr(expansionAddr))

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
