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

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.bigquery import \
  BigQueryEnrichmentHandler


class TestBigQueryEnrichment(unittest.TestCase):
  def setUp(self) -> None:
    self.project = 'google.com:clouddfe'
    self.query_template = (
        "SELECT * FROM "
        "`google.com:clouddfe.my_ecommerce.product_details`"
        " WHERE id = '{}'")
    self.condition_template = "id = '{}'"

  def test_valid_params(self):
    fields = ['id']
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        query_template=self.query_template,
        fields=fields,
        condition_template=self.condition_template,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create(requests)
          | "Enrichment" >> Enrichment(handler)
          | "Output" >> WriteToText('output.txt'))


if __name__ == '__main__':
  unittest.main()