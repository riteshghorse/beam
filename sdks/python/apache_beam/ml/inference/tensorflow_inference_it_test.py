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

"""End-to-End test for Tensorflow Inference"""

from typing import Tuple
import logging
from typing import List
import unittest
import uuid


import apache_beam as beam
from apache_beam.examples.inference.sklearn_japanese_housing_regression import parse_known_args
from apache_beam.examples.inference.sklearn_mnist_classification import PostProcessor
from apache_beam.ml.inference.base import KeyedModelHandler, RunInference
from apache_beam.ml.inference.sklearn_inference import ModelFileType, SklearnModelHandlerNumpy
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.runners.runner import PipelineResult

from apache_beam.testing.test_pipeline import TestPipeline

class TensorflowInference(unittest.TestCase):
  def process_input(self, row: str) -> Tuple[int, List[int]]:
    data = row.split(',')
    label, pixels = int(data[0]), data[1:]
    pixels = [int(pixel) for pixel in pixels]
    return label, pixels

  def _run(self,
      argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
    """
    Args:
      argv: Command line arguments defined for this example.
      save_main_session: Used for internal testing.
      test_pipeline: Used for internal testing.
    """
    known_args, pipeline_args = parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # In this example we pass keyed inputs to RunInference transform.
    # Therefore, we use KeyedModelHandler wrapper over SklearnModelHandlerNumpy.
    model_loader = KeyedModelHandler(
        SklearnModelHandlerNumpy(
            model_file_type=ModelFileType.PICKLE,
            model_uri=known_args.model_path))

    pipeline = test_pipeline
    if not test_pipeline:
      pipeline = beam.Pipeline(options=pipeline_options)

    label_pixel_tuple = (
        pipeline
        | "ReadFromInput" >> beam.io.ReadFromText(known_args.input)
        | "PreProcessInputs" >> beam.Map(self.process_input))

    predictions = (
        label_pixel_tuple
        | "RunInference" >> RunInference(model_loader)
        | "PostProcessOutputs" >> beam.ParDo(PostProcessor()))

    _ = predictions | "WriteOutput" >> beam.io.WriteToText(
        known_args.output, shard_name_template='', append_trailing_newlines=True)

    result = pipeline.run()
    result.wait_until_finish()
    return result
      
  def test_sklearn_mnist_classification(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    input_file = 'gs://apache-beam-ml/testing/inputs/it_mnist_data.csv'
    output_file_dir = 'gs://temp-storage-for-end-to-end-tests'
    output_file = '/'.join([output_file_dir, str(uuid.uuid4()), 'result.txt'])
    model_path = ''
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'model_path': model_path,
    }

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()