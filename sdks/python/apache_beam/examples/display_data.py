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

"""A display data example."""

# pytype: skip-file

import argparse
import logging
import apache_beam as beam
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline import Pipeline
from apache_beam.transforms.display import DisplayDataItem

# class SimpleUpperCase(beam.DoFn):
#   def display_data(self):
#     parent_dd = super().display_data()
#     parent_dd[
#         "my_custom_display_data_key"
#     ] = beam.transforms.display.DisplayDataItem(
#         "my_custom_display_data_item_value",
#         label="my_custom_display_data_item_label",
#     )
#     return parent_dd

#   def process(self, pcoll):
#     yield pcoll.upper()


class ParentSimpleUpperCaseTransform(beam.PTransform):
  def expand(self, p):
    self.p = p
    return p | beam.Create([None])

  def display_data(self):  # type: () -> dict
    parent_dd = super().display_data()
    parent_dd['p_dd_string'] = DisplayDataItem(
        'p_dd_string_value', label='p_dd_string_label')
    parent_dd['p_dd_string_2'] = DisplayDataItem('p_dd_string_value_2')
    parent_dd['p_dd_bool'] = DisplayDataItem(True, label='p_dd_bool_label')
    parent_dd['p_dd_int'] = DisplayDataItem(1, label='p_dd_int_label')
    return parent_dd


class SimpleUpperCaseTransform(ParentSimpleUpperCaseTransform):
  def __init__(self):
    super().__init__()

  def expand(self, pcoll):
    self.p = pcoll
    return pcoll | beam.Create([None])

  def display_data(self):
    parent_dd = super().display_data()
    parent_dd[
        "my_custom_display_data_key"] = beam.transforms.display.DisplayDataItem(
            "my_custom_display_data_item_value",
            label="my_custom_display_data_item_label",
        )
    # print("test dd:", parent_dd)
    return parent_dd


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  pipeline = beam.Pipeline(options=pipeline_options)
  _ = (pipeline | SimpleUpperCaseTransform())
  _ = pipeline.run()


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
