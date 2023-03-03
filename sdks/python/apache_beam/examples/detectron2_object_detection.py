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
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence

from detectron2.data.transforms import ResizeShortestEdge
# from detectron2.checkpoint import DetectionCheckpointer

from torchvision.models.detection import maskrcnn_resnet50_fpn


def custom_tensor_inference_fn(
    batch: Sequence[torch.Tensor],
    model: torch.nn.Module,
    device: str,
    inference_args: Optional[Dict[str,
                                  Any]] = None) -> Iterable[PredictionResult]:
  # torch.no_grad() mitigates GPU memory issues
  # https://github.com/apache/beam/issues/22811
  with torch.no_grad():
    # if batch[0][0]["images"].device != device:

    # batch = batch.to(device)
    # batched_tensors = _convert_to_device(batched_tensors, device)
    outputs = []
    for b in batch:
      predictions = model(b, **inference_args)
      if isinstance(predictions, dict):
        # Go from one dictionary of type: {key_type1: Iterable<val_type1>,
        # key_type2: Iterable<val_type2>, ...} where each Iterable is of
        # length batch_size, to a list of dictionaries:
        # [{key_type1: value_type1, key_type2: value_type2}]
        predictions_per_tensor = [
            dict(zip(predictions.keys(), v))
            for v in zip(*predictions.values())
        ]
        outputs.append(
            PredictionResult(x, y) for x, y in zip(b, predictions_per_tensor))
      outputs.append(PredictionResult(x, y) for x, y in zip(b, predictions))
    return outputs


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
  # model = build_model(cfg)

  #For Danny
  #Starting from here and til PytorchModelHandlerTensor section is just a demonstration
  #how this model actually works. I think this will help to figure out how to make
  #same model workable with pipeline.
  #Load weights, set device, eval mode on
  # model.load_state_dict(torch.load(known_args.weights))
  # checkpointer = DetectionCheckpointer(model, save_dir="./")
  # checkpointer.save("model_999")
  # model.to(torch.device('cuda'))
  # model.eval()

  #Sample image.
  # with open(known_args.input) as f:
  #   lines = f.readlines()
  # image = read_image(image_file_name=str.strip(lines[0]))
  # model_input = preprocess_image(image[1])
  #print(model_input)

  #For Danny
  #A demonstration how to do inference on a batch of 2. In order to compare between TRT and PyT
  #we also need to infer PyT with batch size of 16. So the same logic of assembling batches for execution
  #will apply here. I doubt T4 will be able to infer with bs of 16 in case of PyT. Please adjust code
  #to make sure pipelined runs can take advantage of batch execution. If bs 16 is too much for PyT
  #to handle, lower it to bs 8. Thank you! Uncomment to see how batch execution works.

  #second_image = read_image(image_file_name=str.strip(lines[1]))
  #model_input_2 = preprocess_image(second_image[1])
  #model_input.append(model_input_2[0])
  #print(model_input)

  #Infer
  # with torch.no_grad():
  #   outputs = model(model_input)
  #   print(outputs)

  #For Danny
  #Not sure how to make it work with RunInference since as input model takes list
  #of form [{"image": torch_image, "height": raw_height, "width": raw_width}]
  #You probably know how to make it run. Uncomment and adjust as needed please.

  #Error is TypeError: forward() missing 1 required positional argument: 'batched_inputs' [while running 'PyTorchRunInference/BeamML_RunInference']
  #Apparently no input is passed to forward function for some reason.
  #Something under the hood of RunInference or model handlers.

  model_handler = PytorchModelHandlerTensor(
      state_dict_path=known_args.weights,
      model_class=build_model,
      model_params={"cfg": cfg},
      device='GPU',
      inference_fn=custom_tensor_inference_fn)

  with beam.Pipeline(options=pipeline_options) as p:
    filename_value_pair = (
        p
        | 'ReadImageNames' >> beam.io.ReadFromText(known_args.input)
        | 'ReadImageData' >> beam.Map(
            lambda image_name: read_image(
                image_file_name=image_name, path_to_dir=known_args.images_dir))
        | 'PreprocessImages' >> beam.MapTuple(
            lambda file_name, data: (file_name, preprocess_image(data))))
    predictions = (
        filename_value_pair
        |
        'PyTorchRunInference' >> RunInference(KeyedModelHandler(model_handler)))

    _ = predictions | "WriteOutput" >> beam.io.WriteToText(
        known_args.output,
        shard_name_template='',
        append_trailing_newlines=True)

    result = p.run()
    result.wait_until_finish()
    return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
