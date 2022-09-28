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

package xlang

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*config)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*DataframePl)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*Payload)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*ArgStruct)(nil)).Elem())
}

type config struct {
	dpl           DataframePl
	expansionAddr string
}

type configOption func(*config)

// WithExpansionAddr sets an URL for a Python expansion service.
func WithExpansionAddr(expansionAddr string) configOption {
	return func(c *config) {
		c.expansionAddr = expansionAddr
	}
}

// WithIndexes sets an URL for a Python expansion service.
func WithIndexes() configOption {
	return func(c *config) {
		c.dpl.IncludeIndexes = true
	}
}

type DataframePl struct {
	Fn             beam.PythonCallableSource `beam:"func"`
	IncludeIndexes bool                      `beam:"include_indexes"`
}

type ArgStruct struct {
	args []string
}

type Payload struct {
	Constructor string      `beam:"constructor"`
	Args        ArgStruct   `beam:"args"`
	Kwargs      DataframePl `beam:"kwargs"`
}

func DataframeTransform(s beam.Scope, fn string, col beam.PCollection, outT reflect.Type, opts ...configOption) beam.PCollection {
	s.Scope("xlang.DataframeTransform")
	// beam.PythonCallableSource(beam.NewPythonCode(fn))
	cfg := config{
		dpl: DataframePl{Fn: beam.PythonCallableSource(fn)},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	// TODO: load automatic expansion service here
	if cfg.expansionAddr == "" {
		panic("no expansion service address provided for xlang.DataframeTransform(), pass xlang.WithExpansionAddr(address) as a param.")
	}
	pal := Payload{
		Constructor: "apache_beam.dataframe.transforms.DataframeTransform",
		Kwargs:      cfg.dpl,
		Args:        ArgStruct{},
	}
	// pet := beam.NewPythonExternalTransform("apache_beam.dataframe.transforms.DataframeTransform")
	// pet.WithKwargs(map[string]any{
	// 	"Func":           beam.PythonCallableSource(beam.PythonCode(fn)),
	// 	"IncludeIndexes": cfg.includeIndexes,
	// })
	pl := beam.CrossLanguagePayload(pal)
	// namedInputs := map[string]beam.PCollection{"pcol1": col}
	result := beam.CrossLanguage(s, "beam:transforms:python:fully_qualified_named", pl, cfg.expansionAddr, beam.UnnamedInput(col), beam.UnnamedOutput(typex.New(outT)))
	return result[beam.UnnamedOutputTag()]

}
