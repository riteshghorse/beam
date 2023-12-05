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

import unittest
from typing import Union, Tuple

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.io.requestresponseio_it_test import EchoITOptions, _HTTP_ENDPOINT_ADDRESS_FLAG, EchoHTTPCaller, _PAYLOAD, \
  EchoRequest


class TestEnrichment(unittest.TestCase):
  options: Union[EchoITOptions, None] = None
  client: Union[EchoHTTPCaller, None] = None

  @classmethod
  def setUpClass(cls) -> None:
    cls.options = EchoITOptions()
    http_endpoint_address = cls.options.http_endpoint_address
    http_endpoint_address = 'http://localhost:8080'
    if not http_endpoint_address or http_endpoint_address == '':
      raise unittest.SkipTest(f'{_HTTP_ENDPOINT_ADDRESS_FLAG} is required.')
    cls.client = EchoHTTPCaller(http_endpoint_address)

  @classmethod
  def _get_client_and_options(cls) -> Tuple[EchoHTTPCaller, EchoITOptions]:
    assert cls.options is not None
    assert cls.client is not None
    return cls.client, cls.options

  def test_http_enrichment(self):
    client, options = TestEnrichment._get_client_and_options()
    req = EchoRequest(id=options.never_exceed_quota_id, payload=_PAYLOAD)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      output = (
          test_pipeline
          | 'Create PCollection' >> beam.Create([req])
          | 'Enrichment Transform' >> Enrichment(client))
      self.assertIsNotNone(output)


if __name__ == '__main__':
  unittest.main()
