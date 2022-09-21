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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*Payload)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*config)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*ArgStruct)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*KwargsStruct)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*InfPayload)(nil)).Elem())
}

type Payload struct {
	args   []any          `beam:"args"`
	kwargs map[string]any `beam.:"kwargs"`
}

type config struct {
	pyld          InfPayload
	expansionAddr string
}

type configOption func(*config)

// Sets keyword arguments for the python transform parameters.
func WithKwarg(kwargs KwargsStruct) configOption {
	return func(c *config) {
		c.pyld.Kwargs = kwargs
	}
}

// Sets arguments for the python transform parameters
func WithArgs(args []string) configOption {
	return func(c *config) {
		c.pyld.Args.args = append(c.pyld.Args.args, args...)
	}
}

// A URL for a Python expansion service.
func WithExpansionAddr(expansionAddr string) configOption {
	return func(c *config) {
		c.expansionAddr = expansionAddr
	}
}

type ArgStruct struct {
	args []string
}

var pt = typex.New(reflectx.PythonCallable)

type KwargsStruct struct {
	ModelHandlerProvider pt.Type() `beam:"model_handler_provider"`
	ModelURI             string  `beam:"model_uri"`
}
type InfPayload struct {
	Constructor string       `beam:"constructor"`
	Args        ArgStruct    `beam:"args"`
	Kwargs      KwargsStruct `beam:"kwargs"`
}

type PythonCallableSource struct {
	Code string
}

// Actual RunInference
func RunInference(s beam.Scope, modelLoader string, col beam.PCollection, outT reflect.Type, opts ...configOption) beam.PCollection {
	s.Scope("ml.inference.RunInference")
	// riPyld := &Payload{
	// 	args:   []any{},
	// 	kwargs: make(map[string]any),
	// }

	riPyld := InfPayload{
		Constructor: "apache_beam.ml.inference.base.RunInference.from_callable",
	}
	cfg := config{pyld: riPyld}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.pyld.Kwargs.ModelHandlerProvider = PythonCallableSource{modelLoader}

	// cfg.pyld.Kwargs.ModelHandlerProvider = beam.PythonCallableSource(beam.PythonCode(modelLoader))
	// TODO: load automatic expansion service here
	if cfg.expansionAddr == "" {
		panic("no expansion service address provided for inference.RunInference(), pass inference.WithExpansionAddr(address) as a param.")
	}

	// pet := beam.NewPythonExternalTransform("apache_beam.ml.inference.base.RunInference.from_callable")

	// pet.WithArgs(cfg.pyld.args)
	// pet.WithKwargs(cfg.pyld.kwargs)
	pl := beam.CrossLanguagePayload(cfg.pyld)
	namedInputs := map[string]beam.PCollection{"pcol1": col}
	result := beam.CrossLanguage(s, "beam:transforms:python:fully_qualified_named", pl, cfg.expansionAddr, namedInputs, beam.UnnamedOutput(typex.New(outT)))
	return result[beam.UnnamedOutputTag()]
}
