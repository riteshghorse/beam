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

//TODO: inference package has the cross language implementation of RunInference API implemented in Python SDK.
package inference

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/python"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*config)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*argStruct)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*KwargStruct)(nil)).Elem())
	beam.RegisterType(outputT)
}

var outputT = reflect.TypeOf((*PredictionResult)(nil)).Elem()

// PredictionResult represents the result of a prediction obtained from Python's RunInference API.
type PredictionResult struct {
	Example   []int64 `beam:"example"`
	Inference int32   `beam:"inference"`
}

type config struct {
	kwargs        KwargStruct
	args          argStruct
	expansionAddr string
}

type configOption func(*config)

// Sets keyword arguments for the python transform parameters.
func WithKwarg(kwargs KwargStruct) configOption {
	return func(c *config) {
		c.kwargs = kwargs
	}
}

// Sets arguments for the python transform parameters
func WithArgs(args []string) configOption {
	return func(c *config) {
		c.args.args = append(c.args.args, args...)
	}
}

// A URL for a Python expansion service.
func WithExpansionAddr(expansionAddr string) configOption {
	return func(c *config) {
		c.expansionAddr = expansionAddr
	}
}

type argStruct struct {
	args []string
}

// KwargStruct represents
type KwargStruct struct {
	ModelHandlerProvider python.CallableSource `beam:"model_handler_provider"`
	ModelURI             string                `beam:"model_uri"`
}

// Actual RunInference
func RunInference(s beam.Scope, modelLoader string, col beam.PCollection, opts ...configOption) beam.PCollection {
	s.Scope("ml.inference.RunInference")

	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.kwargs.ModelHandlerProvider = python.CallableSource(modelLoader)
	// TODO: load automatic expansion service here
	if cfg.expansionAddr == "" {
		panic("no expansion service address provided for inference.RunInference(), pass inference.WithExpansionAddr(address) as a param.")
	}
	pet := python.NewExternalTransform[argStruct, KwargStruct]("apache_beam.ml.inference.base.RunInference.from_callable")
	pet.WithKwargs(cfg.kwargs)
	pet.WithArgs(cfg.args)
	pl := beam.CrossLanguagePayload(pet)
	named := map[string]beam.PCollection{"main": col}
	result := beam.CrossLanguage(s, "beam:transforms:python:fully_qualified_named", pl, cfg.expansionAddr, named, beam.UnnamedOutput(typex.New(outputT)))
	return result[beam.UnnamedOutputTag()]
}

// write a run inference function with output key type in signature
func RunInferenceWithKV(s beam.Scope, modelLoader string, col beam.PCollection, outT reflect.Type, opts ...configOption) beam.PCollection {
	s.Scope("ml.inference.RunInferenceWithKV")

	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.kwargs.ModelHandlerProvider = python.CallableSource(modelLoader)
	// TODO: load automatic expansion service here
	if cfg.expansionAddr == "" {
		panic("no expansion service address provided for inference.RunInference(), pass inference.WithExpansionAddr(address) as a param.")
	}
	pet := python.NewExternalTransform[argStruct, KwargStruct]("apache_beam.ml.inference.base.RunInference.from_callable")
	pet.WithKwargs(cfg.kwargs)
	pet.WithArgs(cfg.args)
	pl := beam.CrossLanguagePayload(pet)
	outputKV := typex.NewKV(typex.New(outT), typex.New(outputT))
	result := beam.CrossLanguage(s, "beam:transforms:python:fully_qualified_named", pl, cfg.expansionAddr, beam.UnnamedInput(col), beam.UnnamedOutput(outputKV))
	return result[beam.UnnamedOutputTag()]
}
