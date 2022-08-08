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

//TODO(riteshghorse): are there constant model handlers?

// type PythonCallableSource struct {
// 	PythonCallableCode string
// }

// func NewPythonCallableSource(code string) PythonCallableSource {
// 	return PythonCallableSource{PythonCallableCode: code}
// }

type Kwargs struct {
	ModelHandlerProvider string `beam:"model_handler_provider"`
	ModelURI             string `beam:"model_uri`
}
type payload struct {
	Constructor string            `beam:"constructor"`
	Args        map[string]string `beam:"args"`
	Kwargs      Kwargs            `beam:"kwargs"`
}

// func (p *payload) addKwargs(key string, value string) {
// 	p.Kwargs[key] = value
// }

type config struct {
	pyld          payload
	expansionAddr string
}

type configOption func(*config)

// Sets keyword arguments for the python transform parameters.
func WithKwarg(kwargs Kwargs) configOption {
	return func(c *config) {
		c.pyld.Kwargs = kwargs
	}
}

// Sets arguments for the python transform parameters
func WithArgs(args map[string]string) configOption {
	return func(c *config) {
		c.pyld.Args = args
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

	riPyld := &payload{
		Constructor: "apache_beam.ml.inference.base.RunInference.from_callable",
		Args:        make(map[string]string),
		Kwargs:      Kwargs{},
	}
	// riPyld.addKwargs("model_handler_provider", modelLoader)
	cfg := config{pyld: *riPyld}
	for _, opt := range opts {
		opt(&cfg)
	}
	// riPyld.addKwargs("model_handler_provider", modelLoader)
	cfg.pyld.Kwargs.ModelHandlerProvider = modelLoader
	// TODO: load automatic expansion service here
	if cfg.expansionAddr == "" {
		panic("no expansion service address provided for inference.RunInference(), pass inference.WithExpansionAddr(address) as a param.")
	}
	pl := beam.CrossLanguagePayload(cfg.pyld)
	namedInputs := map[string]beam.PCollection{"pcol1": col}
	result := beam.CrossLanguage(s, "beam:transforms:python:fully_qualified_named", pl, cfg.expansionAddr, namedInputs, beam.UnnamedOutput(typex.New(outT)))
	return result[beam.UnnamedOutputTag()]
}
