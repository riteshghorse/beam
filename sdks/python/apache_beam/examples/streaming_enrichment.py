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

# pytype: skip-file

import json

import logging
import time

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler

_LOGGER = logging.getLogger(__name__)


class BytesToRow(beam.DoFn):
  def process(self, element, *args, **kwargs):
    value = json.loads(element.decode('utf-8'))
    time.sleep(2)
    for _ in range(10000):
      yield beam.Row(**value)


def run(save_main_session=True):
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  project_id, instance_id, table_id = \
    "google.com:clouddfe", "bigtable-enrichment", "riteshghorse-notebook"
  subscription = \
    'projects/google.com:clouddfe/subscriptions/' \
    'riteshghorse-enrichment-example-sub'

  row_key = 'customer_id'
  bigtable_enrichment = BigTableEnrichmentHandler(
      project_id, instance_id, table_id, row_key)
  with beam.Pipeline(options=pipeline_options) as p:
    _ = (
        p
        | "read from pubsub" >> ReadFromPubSub(subscription=subscription)
        | "BytesToRow" >> beam.ParDo(BytesToRow())
        | "Enrichment" >> Enrichment(bigtable_enrichment))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
