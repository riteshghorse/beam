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
)

type Payload struct {
	args   []any          `beam:"args"`
	kwargs map[string]any `beam.:"kwargs"`
}

type config struct {
	pyld          Payload
	expansionAddr string
}

type configOption func(*config)

// Sets keyword arguments for the python transform parameters.
func WithKwarg(kwargs map[string]any) configOption {
	return func(c *config) {
		for k, v := range kwargs {
			c.pyld.kwargs[k] = v
		}
	}
}

// Sets arguments for the python transform parameters
func WithArgs(args []any) configOption {
	return func(c *config) {
		c.pyld.args = append(c.pyld.args, args...)
	}
}

// A URL for a Python expansion service.
func WithExpansionAddr(expansionAddr string) configOption {
	return func(c *config) {
		c.expansionAddr = expansionAddr
	}
}

// Actual RunInference
func RunInference(s beam.Scope, modelLoader string, col beam.PCollection, outT reflect.Type, opts ...configOption) beam.PCollection {
	s.Scope("ml.inference.RunInference")
	riPyld := &Payload{
		args:   []any{},
		kwargs: make(map[string]any),
	}

	cfg := config{pyld: *riPyld}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.pyld.kwargs["ModelHandlerProvider"] = modelLoader
	// TODO: load automatic expansion service here
	if cfg.expansionAddr == "" {
		panic("no expansion service address provided for inference.RunInference(), pass inference.WithExpansionAddr(address) as a param.")
	}

	pet := beam.NewPythonExternalTransform("apache_beam.ml.inference.base.RunInference.from_callable")

	pet.WithArgs(cfg.pyld.args)
	pet.WithKwargs(cfg.pyld.kwargs)
	pl := beam.CrossLanguagePayload(pet)
	namedInputs := map[string]beam.PCollection{"pcol1": col}
	result := beam.CrossLanguage(s, "beam:transforms:python:fully_qualified_named", pl, cfg.expansionAddr, namedInputs, beam.UnnamedOutput(typex.New(outT)))
	return result[beam.UnnamedOutputTag()]
}
