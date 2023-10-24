#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/echo/v1/echo.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x18proto/echo/v1/echo.proto\x12\rproto.echo.v1\"7\n\x0b\x45\x63hoRequest\x12\x0e\n\x02id\x18\x01 \x01(\tR\x02id\x12\x18\n\x07payload\x18\x02 \x01(\x0cR\x07payload\"8\n\x0c\x45\x63hoResponse\x12\x0e\n\x02id\x18\x01 \x01(\tR\x02id\x12\x18\n\x07payload\x18\x02 \x01(\x0cR\x07payload2P\n\x0b\x45\x63hoService\x12\x41\n\x04\x45\x63ho\x12\x1a.proto.echo.v1.EchoRequest\x1a\x1b.proto.echo.v1.EchoResponse\"\x00\x42;\n*org.apache.beam.testinfra.mockapis.echo.v1Z\rproto/echo/v1b\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.echo.v1.echo_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n*org.apache.beam.testinfra.mockapis.echo.v1Z\rproto/echo/v1'
  _ECHOREQUEST._serialized_start=43
  _ECHOREQUEST._serialized_end=98
  _ECHORESPONSE._serialized_start=100
  _ECHORESPONSE._serialized_end=156
  _ECHOSERVICE._serialized_start=158
  _ECHOSERVICE._serialized_end=238
# @@protoc_insertion_point(module_scope)
