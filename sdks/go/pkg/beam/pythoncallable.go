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

package beam

import (
	"fmt"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

const (
	pythonCallableUrn = "beam:logical_type:python_callable:v1"
)

var (
	pcsType        = reflect.TypeOf((*PythonCallableSource)(nil)).Elem()
	pcsStorageType = reflectx.String
)

func init() {
	RegisterType(pcsType)
	// RegisterType(pcsStorageType)
	RegisterSchemaProviderWithURN(pcsType, &PythonCallableSourceProvider{}, pythonCallableUrn)
}

type PythonCallableSource string

type PythonCallableSourceProvider struct{}

func (p *PythonCallableSourceProvider) FromLogicalType(rt reflect.Type) (reflect.Type, error) {
	if rt != pcsType {
		return nil, fmt.Errorf("unable to provide schema.LogicalType for type %v, want %v", rt, pcsType)
	}
	return pcsStorageType, nil
}

func (p *PythonCallableSourceProvider) BuildEncoder(rt reflect.Type) (func(interface{}, io.Writer) error, error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}

	// enc := exec.MakeElementDecoder(coder.NewString())
	// if err != nil {
	// 	return nil, err
	// }
	return func(iface interface{}, w io.Writer) error {
		v := iface.(PythonCallableSource)
		return coder.EncodeStringUTF8(string(v), w)
	}, nil
}

func (p *PythonCallableSourceProvider) BuildDecoder(rt reflect.Type) (func(io.Reader) (interface{}, error), error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}
	// dec, err := coder.RowDecoderForStruct(pcsStorageType)
	// if err != nil {
	// 	return nil, err
	// }
	return func(r io.Reader) (interface{}, error) {
		// s, err := dec(r)
		// if err != nil {
		// 	return nil, err
		// }
		// tn := s.(pcsStorage)

		// return PythonCode(tn.Code), nil
		s, err := coder.DecodeStringUTF8(r)
		if err != nil {
			return nil, err
		}
		return PythonCallableSource(s), nil
	}, nil
}
