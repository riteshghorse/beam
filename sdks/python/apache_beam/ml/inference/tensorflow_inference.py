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

import logging
from collections import defaultdict
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Union

import sys 
import torch
import tensorflow as tf
import numpy
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.utils.annotations import experimental

__all__ = [
    'TFModelHandlerNumpy',
]

TensorInferenceFn = Callable[
    [tf.Module, Sequence[numpy.ndarray], Optional[Dict[str, Any]]],
    Iterable[PredictionResult]]

KeyedTensorInferenceFn = Callable[[
    Sequence[Dict[str, torch.Tensor]],
    torch.nn.Module,
    str,
    Optional[Dict[str, Any]]
],
Iterable[PredictionResult]]


def _load_model(model_uri):
  from tensorflow import keras
  return keras.models.load_model(model_uri)

def _convert_to_result(
    batch: Iterable, predictions: Union[Iterable, Dict[Any, Iterable]]
) -> Iterable[PredictionResult]:
  if isinstance(predictions, dict):
    # Go from one dictionary of type: {key_type1: Iterable<val_type1>,
    # key_type2: Iterable<val_type2>, ...} where each Iterable is of
    # length batch_size, to a list of dictionaries:
    # [{key_type1: value_type1, key_type2: value_type2}]
    predictions_per_tensor = [
        dict(zip(predictions.keys(), v)) for v in zip(*predictions.values())
    ]
    return [
        PredictionResult(x, y) for x, y in zip(batch, predictions_per_tensor)
    ]
  return [PredictionResult(x, y) for x, y in zip(batch, predictions)]


def default_tensor_inference_fn(
    model: tf.Module,
    batch: Sequence[numpy.ndarray],
    inference_args: Optional[Dict[str,Any]] = None) -> Iterable[PredictionResult]:
  vectorized_batch = numpy.stack(batch, axis=0)
  return model.predict(vectorized_batch)


class TFModelHandlerNumpy(ModelHandler[numpy.ndarray,
                                             PredictionResult,
                                             tf.Module]):
  def __init__(
      self,
      model_uri: str,
      device: str = 'CPU',
      *,
      inference_fn: TensorInferenceFn = default_tensor_inference_fn):
    self._model_uri = model_uri
    self._inference_fn = inference_fn
    self._device = device

  def load_model(self) -> tf.Module:
    """Loads and initializes a Pytorch model for processing."""
    return _load_model(self._model_uri)

  def run_inference(
      self,
      batch: Sequence[numpy.ndarray],
      model: tf.Module,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Tensors and returns an Iterable of
    Tensor Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.

    Args:
      batch: A sequence of Tensors. These Tensors should be batchable, as this
        method will call `torch.stack()` and pass in batched Tensors with
        dimensions (batch_size, n_features, etc.) into the model's forward()
        function.
      model: A PyTorch model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched

    Returns:
      An Iterable of type PredictionResult.
    """
    predictions = self._inference_fn(model, batch, inference_args)
    return _convert_to_result(batch, predictions)

  def get_num_bytes(self, batch: Sequence[numpy.ndarray]) -> int:
    """
    Returns:
      The number of bytes of data for a batch of Tensors.
    """
    return sum(sys.getsizeof(element) for element in batch)

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_PyTorch'

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    pass


def default_keyed_tensor_inference_fn(
    batch: Sequence[Dict[str, numpy.ndarray]],
    model: tf.Module,
    device: str,
    inference_args: Optional[Dict[str,
                                  Any]] = None) -> Iterable[PredictionResult]:
  # If elements in `batch` are provided as a dictionaries from key to Tensors,
  # then iterate through the batch list, and group Tensors to the same key
  key_to_tensor_list = defaultdict(list)

  # torch.no_grad() mitigates GPU memory issues
  # https://github.com/apache/beam/issues/22811
  with torch.no_grad():
    for example in batch:
      for key, value in example.items():
        key_to_tensor_list[key].append(value)
    key_to_batched_tensors = {}
    for key in key_to_tensor_list:
      batched_tensors = numpy.stack(key_to_tensor_list[key])
      # batched_tensors = _convert_to_device(batched_tensors, device)
      key_to_batched_tensors[key] = batched_tensors
    predictions = model(**key_to_batched_tensors, **inference_args)

    return _convert_to_result(batch, predictions)


def make_keyed_tensor_model_fn(model_fn: str) -> KeyedTensorInferenceFn:
  """
  Produces a KeyedTensorInferenceFn that uses a method of the model other that
  the forward() method.

  Args:
    model_fn: A string name of the method to be used. This is accessed through
      getattr(model, model_fn)
  """
  def attr_fn(
      batch: Sequence[torch.Tensor],
      model: torch.nn.Module,
      device: str,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    # If elements in `batch` are provided as a dictionaries from key to Tensors,
    # then iterate through the batch list, and group Tensors to the same key
    key_to_tensor_list = defaultdict(list)

    # torch.no_grad() mitigates GPU memory issues
    # https://github.com/apache/beam/issues/22811
    with torch.no_grad():
      for example in batch:
        for key, tensor in example.items():
          key_to_tensor_list[key].append(tensor)
      key_to_batched_tensors = {}
      for key in key_to_tensor_list:
        batched_tensors = torch.stack(key_to_tensor_list[key])
        batched_tensors = _convert_to_device(batched_tensors, device)
        key_to_batched_tensors[key] = batched_tensors
        pred_fn = getattr(model, model_fn)
      predictions = pred_fn(**key_to_batched_tensors, **inference_args)
    return _convert_to_result(batch, predictions)

  return attr_fn


@experimental(extra_message="No backwards-compatibility guarantees.")
class TFModelHandlerKeyedNumpy(ModelHandler[Dict[str, numpy.ndarray],
                                                  PredictionResult,
                                                  tf.Module]):
  def __init__(
      self,
      model_uri: str,
      device: str = 'CPU',
      *,
      inference_fn: KeyedTensorInferenceFn = default_keyed_tensor_inference_fn):
    """Implementation of the ModelHandler interface for TF.
    """
    self._model_uri = model_uri
    self._device = device
    self._inference_fn = inference_fn

  def load_model(self) -> tf.Module:
    """Loads and initializes a Pytorch model for processing."""
    return _load_model(self._model_uri)

  def run_inference(
      self,
      batch: Sequence[Dict[str, numpy.ndarray]],
      model: tf.Module,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Keyed Numpy array and returns an Iterable of
    numpy Predictions.

    For the same key across all examples, this will stack all numpy array values
    in a vectorized format to optimize the inference call.

    Args:
      batch: A sequence of keyed Tensors. These Tensors should be batchable,
        as this method will call `torch.stack()` and pass in batched Tensors
        with dimensions (batch_size, n_features, etc.) into the model's
        forward() function.
      model: A PyTorch model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched

    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args

    return self._inference_fn(batch, model, self._device, inference_args)

  def get_num_bytes(self, batch: Sequence[torch.Tensor]) -> int:
    """
    Returns:
       The number of bytes of data for a batch of Dict of Tensors.
    """
    # If elements in `batch` are provided as a dictionaries from key to Tensors
    return sum(
        (el.element_size() for tensor in batch for el in tensor.values()))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_PyTorch'

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    pass
