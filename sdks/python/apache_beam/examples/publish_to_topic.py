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
import argparse
import json
import logging
import random
import time

import apache_beam as beam
from apache_beam.io import WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.periodicsequence import PeriodicImpulse

_LOGGER = logging.getLogger(__name__)


class ConvertToBytes(beam.DoFn):
  def process(self, element, *args, **kwargs):
    value = json.dumps(element).encode('utf-8')
    yield value


def run(argv=None):
  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  messages = [{
      'sale_id': i,
      'customer_id': i,
      'product_id': i,
      'quantity': i,
      'price': i * 100
  } for i in range(1, 9)]
  start_time = time.time()
  end_time = start_time + (60 * 5)
  with beam.Pipeline(options=pipeline_options) as p:
    _ = (
        p
        | 'Stream Data' >> PeriodicImpulse(
            start_time, end_time, fire_interval=0.001)
        | 'Create' >> beam.Map(lambda x: messages[random.randint(0, 7)])
        | 'Convert' >> beam.ParDo(ConvertToBytes())
        | 'Write to PubSub' >> WriteToPubSub(
            'projects/google.com:clouddfe/topics/'
            'riteshghorse-enrichment-example'))


if __name__ == '__main__':
  run()
