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
# import unittest
#
# from apache_beam.transforms.enrichment_handlers.bigquery import \
#   BigQueryEnrichmentHandler
#
#
# class TestBigQueryEnrichmentHandler(unittest.TestCase):
#   def setUp(self) -> None:
#     self.project = 'google.com:clouddfe'
#
#   def test_valid_params(self):
#     fields = ['id']
#     handler = BigQueryEnrichmentHandler(
#         project=self.project,
#         query_template=self.query_template,
#         fields=fields,
#         condition_template='id = %s',
#         min_batch_size=1,
#         max_batch_size=100,
#     )