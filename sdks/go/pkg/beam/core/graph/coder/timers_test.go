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

package coder

// import (
// 	"bytes"
// 	"testing"
// 	"time"

// 	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
// )

// func TestEncodeTimer(t *testing.T) {
// 	type args struct {
// 		tm typex.Timers
// 	}
// 	// ww := &bytes.Buffer{}
// 	tests := []struct {
// 		name    string
// 		args    args
// 		wantW   string
// 		wantErr bool
// 	}{
// 		{
// 			name: "encode",
// 			args: args{
// 				tm: typex.Timers{
// 					Key: []byte("basic"),
// 					Tag: "",
// 					// Windows:      ,
// 					Clear:         false,
// 					FireTimestamp: time.Now().Add(time.Second * 2).Unix(),
// 				},
// 			},
// 			wantW:   "timer_test.timer_test",
// 			wantErr: false,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			w := &bytes.Buffer{}
// 			if err := EncodeTimer(tt.args.tm, w); (err != nil) != tt.wantErr {
// 				t.Errorf("EncodeTimer() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			t.Error(w.String())
// 			if gotW := w.String(); gotW != tt.wantW {
// 				t.Errorf("EncodeTimer() = %v, want %v", gotW, tt.wantW)
// 			}
// 		})
// 	}
// }
