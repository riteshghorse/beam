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

"""A pipeline that uses RunInference API to classify MNIST data.

This pipeline takes a text file in which data is comma separated ints. The first
column would be the true label and the rest would be the pixel values. The data
is processed and then a model trained on the MNIST data would be used to perform
the inference. The pipeline writes the prediction to an output file in which
users can then compare against the true label.
"""

import argparse
import logging
from typing import Iterable
from typing import List
from typing import Tuple

import numpy 
import tensorflow as tf

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler, ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerNumpy, TFModelHandlerTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms.core import Map

def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path to save output predictions.')
  parser.add_argument(
      '--model_path',
      dest='model_path',
      required=True,
      help='Path to saved model.')
 
  return parser.parse_known_args(argv)


def run(
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
#   model_loader =  TFModelHandlerNumpy(known_args.model_path)

#   pipeline = test_pipeline
#   if not test_pipeline:
#     pipeline = beam.Pipeline(options=pipeline_options)

#   label_pixel_tuple = (
#       pipeline
#       | "ReadFromInput" >> beam.Create([[1], [2], [3]]))

#   predictions = (
#       label_pixel_tuple
#       | "RunInference" >> RunInference(model_loader))

#   _ = predictions | "WriteOutput" >> beam.io.WriteToText(
#       known_args.output, shard_name_template='', append_trailing_newlines=True)
  
  # model_loader =  KeyedModelHandler(TFModelHandlerNumpy(known_args.model_path))

  # pipeline = test_pipeline
  # if not test_pipeline:
  #   pipeline = beam.Pipeline(options=pipeline_options)

  # label_pixel_tuple = (
  #     pipeline
  #     | "ReadFromInput" >> beam.Create([(1,[1]), (2,[2]), (3, [3])])) 


  # predictions = (
  #     label_pixel_tuple
  #     | "RunInference" >> RunInference(model_loader))

  # _ = predictions | "WriteOutput" >> beam.io.WriteToText(
  #     known_args.output, shard_name_template='', append_trailing_newlines=True)

  model_loader =  TFModelHandlerTensor(known_args.model_path)

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  examples = numpy.array([200, 300, 400], dtype=numpy.float32)
  tf_examples = tf.convert_to_tensor(examples, dtype=tf.float32)
  label_pixel_tuple = (
      pipeline
      | "ReadFromInput" >> beam.Create(tf_examples))


  predictions = (
      label_pixel_tuple
      | "RunInference" >> RunInference(model_loader))

  _ = predictions | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)
  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
