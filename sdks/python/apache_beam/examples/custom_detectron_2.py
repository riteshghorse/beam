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

"""A pipeline that uses RunInference API to perform image segmentation."""

import argparse
import io
import logging
import os
from typing import Iterable
from typing import Iterator
from typing import Optional
from typing import Tuple
import sys
import apache_beam as beam
import torch
from detectron2.modeling import build_model
from detectron2.config import get_cfg
import numpy as np
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor, PytorchModelHandlerTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from PIL import Image
from typing import Sequence
from typing import Any
from typing import List
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Callable
from typing import Union
from apache_beam.ml.inference.base import ModelHandler

from detectron2.data.transforms import ResizeShortestEdge
# from detectron2.checkpoint import DetectionCheckpointer

from torchvision.models.detection import maskrcnn_resnet50_fpn


def read_image(image_file_name: str,
               path_to_dir: Optional[str] = None) -> Tuple[str, Image.Image]:
  if path_to_dir is not None:
    image_file_name = os.path.join(path_to_dir, image_file_name)
  with FileSystems().open(image_file_name, 'r') as file:
    data = Image.open(io.BytesIO(file.read())).convert('RGB')
    return image_file_name, data


def preprocess_image(data: Image.Image):
  #Represent as numpy array
  image = np.asarray(data, dtype=np.float32)
  #To BGR
  image = image[..., ::-1].copy()
  raw_height, raw_width = image.shape[:2]
  #Use Detectron2 image preprocessor
  torch_image = ResizeShortestEdge(
      short_edge_length=[1344, 1344],
      max_size=1344).get_transform(image).apply_image(image)
  #Convert to tensor
  torch_image = torch.as_tensor(
      torch_image.astype("float32").transpose(2, 0, 1))
  #Represent as input format that model accepts
  inputs = [{"image": torch_image, "height": raw_height, "width": raw_width}]
  return inputs


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Path to the text file containing image names.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path where to save output predictions.'
      ' text file.')
  parser.add_argument(
      '--weights',
      dest='weights',
      required=True,
      help="Path to the model's state_dict. "
      "Default state_dict would be maskrcnn_resnet50_fpn.")
  parser.add_argument(
      '--config_file',
      dest='config_file',
      required=True,
      help="Path to the model's state_dict. "
      "Default state_dict would be maskrcnn_resnet50_fpn.")
  parser.add_argument(
      '--images_dir',
      default=None,
      help='Path to the directory where images are stored.'
      'Not required if image names in the input file have absolute path.')
  return parser.parse_known_args(argv)


def _load_model(
    model_class: torch.nn.Module, state_dict_path, device, **model_params):
  model = model_class(**model_params)

  if device == torch.device('cuda') and not torch.cuda.is_available():
    logging.warning(
        "Model handler specified a 'GPU' device, but GPUs are not available. " \
        "Switching to CPU.")
    device = torch.device('cpu')

  file = FileSystems.open(state_dict_path, 'rb')
  try:
    logging.info(
        "Loading state_dict_path %s onto a %s device", state_dict_path, device)
    state_dict = torch.load(file, map_location=device)
  except RuntimeError as e:
    if device == torch.device('cuda'):
      message = "Loading the model onto a GPU device failed due to an " \
        f"exception:\n{e}\nAttempting to load onto a CPU device instead."
      logging.warning(message)
      return _load_model(
          model_class, state_dict_path, torch.device('cpu'), **model_params)
    else:
      raise e

  model.load_state_dict(state_dict)
  model.to(device)
  model.eval()
  logging.info("Finished loading PyTorch model.")
  return (model, device)


def _convert_to_device(examples: Sequence[List[Dict]], device) -> Sequence[List[Dict]]:
  """
  Converts samples to a style matching given device.

  **NOTE:** A user may pass in device='GPU' but if GPU is not detected in the
  environment it must be converted back to CPU.
  """
  for item in examples:
    item[0]["image"].to(device)
  return examples


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
        PredictionResult("prediction", y) for y in predictions_per_tensor
    ]
  return [PredictionResult("prediction", y) for y in predictions]

############################

class CustomPytorchModelHandlerTensor(ModelHandler[List[Dict],
                                             PredictionResult,
                                             torch.nn.Module]):
  def __init__(
      self,
      state_dict_path: str,
      model_class: Callable[..., torch.nn.Module],
      model_params: Dict[str, Any],
      device: str = 'CPU'):
    self._state_dict_path = state_dict_path
    if device == 'GPU':
      logging.info("Device is set to CUDA")
      self._device = torch.device('cuda')
    else:
      logging.info("Device is set to CPU")
      self._device = torch.device('cpu')
    self._model_class = model_class
    self._model_params = model_params

  def load_model(self) -> torch.nn.Module:
    """Loads and initializes a Pytorch model for processing."""
    model, device = _load_model(
        self._model_class,
        self._state_dict_path,
        self._device,
        **self._model_params)
    self._device = device
    return model

  def run_inference(
      self,
      batch: Sequence[List[Dict]],
      model: torch.nn.Module,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    inference_args = {} if not inference_args else inference_args
    # do inference here itself
    batched_list = _convert_to_device(batch, self._device)
    # batched_list = batch
    predictions = []
    with torch.no_grad():
      for bl in batched_list:
        predictions.append(model(bl, **inference_args))
    # TODO: can we skip the returns from here?
    # TODO: log size of input and output.
    return _convert_to_result(batch, predictions)

  def get_num_bytes(self, batch: Sequence[List[Dict]]) -> int:
    """
    Returns:
      The number of bytes of data for a batch of Tensors.
    """
    return sum([sys.getsizeof(el) for el in batch])

  def batch_elements_kwargs(self):
    return {'max_batch_size': 1}
        
  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_PyTorch'

  def validate_inference_args(self, inference_args: Optional[Dict[str, Any]]):
    pass
  
  
def run(argv=None, save_main_session=True):
  """
  Args:
    argv: Command line arguments defined for this example.
    model_class: Reference to the class definition of the model.
                If None, maskrcnn_resnet50_fpn will be used as default .
    model_params: Parameters passed to the constructor of the model_class.
                  These will be used to instantiate the model object in the
                  RunInference API.
    save_main_session: Used for internal testing.
    test_pipeline: Used for internal testing.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True  #skip

  #Leave this part even if using Dataflow
  #It uses Detectron2 specific setup.
  cfg = get_cfg()
  cfg.merge_from_file(known_args.config_file)
  cfg.freeze()

  model_handler = CustomPytorchModelHandlerTensor(
      state_dict_path=known_args.weights,
      model_class=build_model,
      model_params={"cfg": cfg},
      device='GPU')

  with beam.Pipeline(options=pipeline_options) as p:
    filename_value_pair = (
        p
        | 'ReadImageNames' >> beam.io.ReadFromText(known_args.input)
        | 'ReadImageData' >> beam.Map(
            lambda image_name: read_image(
                image_file_name=image_name, path_to_dir=known_args.images_dir))
        | 'PreprocessImages' >> beam.MapTuple(
            lambda file_name, data: (file_name, preprocess_image(data))))
    _ = (
        filename_value_pair
        |
        'PyTorchRunInference' >> RunInference(KeyedModelHandler(model_handler)))

    # _ = predictions | "WriteOutput" >> beam.io.WriteToText(
    #     known_args.output,
    #     shard_name_template='',
    #     append_trailing_newlines=True)

    result = p.run()
    result.wait_until_finish()
    return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
