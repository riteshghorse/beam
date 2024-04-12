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
from apache_beam.transforms.enrichment_handlers.feast_feature_store import \
  FeastFeatureStoreEnrichmentHandler


class TestFeastEnrichmentHandler(unittest.TestCase):
  def setUp(self) -> None:
    self.feature_store_yaml_file = (
        'gs://clouddfe-riteshghorse/'
        'feast-feature-store/repos/ecommerce/'
        'feature_repo/feature_store.yaml')
    self.feature_names = ['distinct_purchased_categories']
    self.feature_service_name = 'model_v1'

  def test_feast_enrichment(self):
    requests = [
        beam.Row(user_id=2, product_id=1),
        beam.Row(user_id=6, product_id=2),
        beam.Row(user_id=9, product_id=3),
    ]
    # expected_fields = []
    handler = FeastFeatureStoreEnrichmentHandler(
        entity_id='user_id',
        feature_store_yaml_path=self.feature_store_yaml_file,
        feature_service_name=self.feature_service_name,
    )

    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | WriteToText('output.txt'))

  def tearDown(self) -> None:
    pass


if __name__ == '__main__':
  unittest.main()
