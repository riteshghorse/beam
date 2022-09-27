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
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*pythonExternalTransform)(nil)))
}

// pythonExternalTransform holds the information required for an External Python Transform.
type pythonExternalTransform struct {
	Constructor string        `beam:"constructor"`
	Args        reflect.Value `beam:"args"`
	Kwargs      reflect.Value `beam:"kwargs"`
}

func NewPythonExternalTransform(constructor string) *pythonExternalTransform {
	return &pythonExternalTransform{Constructor: constructor}
}

// WithArgs builds a struct from the given arguments required by the External Python Transform.
func (p *pythonExternalTransform) WithArgs(args []any) {
	fields := []reflect.StructField{}
	for i, v := range args {
		fields = append(fields, reflect.StructField{Name: fmt.Sprintf("Field%d", i), Type: reflect.TypeOf(v)})
	}

	argStruct := reflect.StructOf(fields)
	values := reflect.New(argStruct).Elem()
	for i := 0; i < argStruct.NumField(); i++ {
		values.Field(i).Set(reflect.ValueOf(args[i]))
	}
	p.Args = values
}

// WithKwargs builds a struct from the given keyworf arguments required by the External Python Transform.
func (p *pythonExternalTransform) WithKwargs(kwargs map[string]any) {
	fields := []reflect.StructField{}
	for k, v := range kwargs {
		name := strings.ToUpper(k)
		// str := "`beam:" + fmt.Sprintf("\"%s\"", k) + "`"
		tag := reflect.StructTag(fmt.Sprintf("`beam:\"%s\"`", k))
		fields = append(fields, reflect.StructField{Name: name, Type: reflect.TypeOf(v), Tag: tag})
	}

	argStruct := reflect.StructOf(fields)
	fmt.Println(argStruct)
	values := reflect.New(argStruct).Elem()
	for i := 0; i < argStruct.NumField(); i++ {
		fmt.Println(i, values.FieldByName(argStruct.Field(i).Name), argStruct.Field(i).Name)
		values.FieldByName(argStruct.Field(i).Name).Set(reflect.ValueOf(kwargs[strings.ToLower(argStruct.Field(i).Name)]))
	}

	fmt.Printf("\n\n%#v", values)
	p.Kwargs = values
}
