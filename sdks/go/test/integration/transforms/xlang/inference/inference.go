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

package inference

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/universal"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/inference"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

func init() {
	beam.RegisterFunction(dropKeyFn)
	beam.RegisterFunction(doPrediction)
}

func RunInference(expansionAddr string) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	beam.Impulse(s)

	inputRow := [][]int64{{0, 0}, {1, 1}}
	input := beam.CreateList(s, inputRow)
	kwargs := inference.KwargStruct{
		ModelURI: "/tmp/staged/sklearn_model",
	}
	output := []inference.PredictionResult{
		{
			Example:   []int64{0, 0},
			Inference: 0,
		},
		{
			Example:   []int64{1, 1},
			Inference: 1,
		},
	}
	outCol := inference.RunInference(s, "apache_beam.ml.inference.sklearn_inference.SklearnModelHandlerNumpy", input, inference.WithKwarg(kwargs), inference.WithExpansionAddr(expansionAddr))
	passert.Equals(s, outCol, output[0], output[1])
	return p
}

func dropKeyFn(_ int64, row inference.PredictionResult) inference.PredictionResult {
	return row
}

func doPrediction(key int64, v []int64) (int64, inference.PredictionResult) {
	return key, inference.PredictionResult{Example: v, Inference: int32(v[0])}
}

func RunInferenceWithKV(expansionAddr string) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	beam.Impulse(s)

	inputRow := [][]int64{{0, 0}, {1, 1}}
	input := beam.CreateList(s, inputRow)
	input = beam.ParDo(s, func(example []int64) (int64, []int64) {
		return example[0], example
	}, input)

	kwargs := inference.KwargStruct{
		ModelURI: "/tmp/staged/sklearn_model",
	}
	// output := []inference.PredictionResult{
	// 	{
	// 		Example:   []int64{0, 0},
	// 		Inference: 0,
	// 	},
	// 	{
	// 		Example:   []int64{1, 1},
	// 		Inference: 1,
	// 	},
	// }

	// ml := strings.Builder{}
	// ml.WriteString("from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy\n")
	// ml.WriteString("from apache_beam.ml.inference.base import KeyedModelHandler\n")
	// ml.WriteString("def get_model_handler(model_uri):\n")
	// ml.WriteString("  return KeyedModelHandler(SklearnModelHandlerNumpy(model_uri))\n")

	modelLoader := "from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy\n"
	modelLoader += "from apache_beam.ml.inference.base import KeyedModelHandler\n"
	modelLoader += "def get_model_handler(model_uri):\n"
	modelLoader += "  return KeyedModelHandler(SklearnModelHandlerNumpy(model_uri))\n"

	a := int64(1)
	outT := reflect.TypeOf(a)
	// fmt.Print(ml.String())
	outCol := inference.RunInferenceWithKV(s, modelLoader, input, outT, inference.WithKwarg(kwargs), inference.WithExpansionAddr(expansionAddr))
	// log.Infof(context.Background(), "%v", outCol.Coder().Type())
	// outCol = beam.ParDo(s, func(ctx context.Context, a int64, row inference.PredictionResult) inference.PredictionResult {
	// 	log.Infof(ctx, "key: %d, value: %v", a, row)
	// 	return row
	// }, outCol)
	// outCol := beam.ParDo(s, doPrediction, input)
	// outCol = beam.ParDo(s, dropKeyFn, outCol)
	debug.Print(s, outCol)
	// passert.Equals(s, outCol, keyedOut)
	return p
}
