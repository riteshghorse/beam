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
)

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

func RunInferenceWithKV(expansionAddr string) *beam.Pipeline {
	p, s := beam.NewPipelineWithRoot()

	beam.Impulse(s)

	inputRow := [][]int64{{0, 0}, {1, 1}}
	input := beam.CreateList(s, inputRow)
	input = beam.ParDo(s, func(example []int64, emit func([]int64, int)) {
		emit(example, int(example[0]))
	}, input)
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
	a := int32(1)
	outT := reflect.TypeOf(a)
	outCol := inference.RunInferenceWithKV(s, "apache_beam.ml.inference.sklearn_inference.SklearnModelHandlerNumpy", input, outT, inference.WithKwarg(kwargs), inference.WithExpansionAddr(expansionAddr))
	passert.Equals(s, outCol, output[0], output[1])
	return p
}
