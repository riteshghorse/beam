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

import unittest
from apache_beam.examples import inference
from apache_beam.ml.inference.sklearn_inference_test import compare_prediction_result


import numpy


from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerNumpy

class FakeTFNumpyModel:
    def predict(self, input: numpy.ndarray):
      return numpy.multiply(input, 10)

class TFRunInferenceTest(unittest.TestCase):
  def test_predict_numpy(self):
    fake_model = FakeTFNumpyModel()
    inference_runner = TFModelHandlerNumpy(model_uri='unused')
    batched_examples = [
        numpy.array([1]), numpy.array([10]), numpy.array([100])
    ]
    expected_predictions = [
        PredictionResult(numpy.array([1]), 10),
        PredictionResult(numpy.array([10]), 100),
        PredictionResult(numpy.array([100]), 1000)
    ]
    inferences = inference_runner.run_inference(batched_examples, fake_model)
    for actual, expected in zip(inferences, expected_predictions):
      self.assertTrue(compare_prediction_result(actual, expected))
    
    
    
    
    
if __name__ == '__main__':
  unittest.main()