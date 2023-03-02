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

"""A pipeline that uses RunInference API to perform object detection with
TensorRT.
"""

import argparse
import io
import os
from typing import Iterable
from typing import Optional
from typing import Tuple
import numpy as np

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.tensorrt_inference import TensorRTEngineHandlerNumPy  # pylint: disable=line-too-long
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from PIL import Image
from detectron2.checkpoint import DetectionCheckpointer

def attach_scale_to_key(data):
  #We need scale in post processing hence package accordingly.
  file_name, image_data = data
  scale, image = image_data
  return ((file_name, scale), image)


def read_image(image_file_name: str,
               path_to_dir: Optional[str] = None) -> Tuple[str, Image.Image]:
  if path_to_dir is not None:
    image_file_name = os.path.join(path_to_dir, image_file_name)
  #For Danny
  #Open a batch of images instead of one.
  with FileSystems().open(image_file_name, 'r') as file:
    data = Image.open(io.BytesIO(file.read())).convert('RGB')
    return image_file_name, data


def preprocess_image(image: Image.Image) -> Tuple[float, np.ndarray]:
  # Get characteristics.
  width, height = image.size

  # Replicates behavior of ResizeShortestEdge augmentation.
  size = 800 * 1.0
  pre_scale = size / min(height, width)
  if height < width:
      newh, neww = size, pre_scale * width
  else:
      newh, neww = pre_scale * height, size

  # If delta between min and max dimensions is so that max sized dimension reaches self.max_size_test
  # before min dimension reaches self.min_size_test, keeping the same aspect ratio. We still need to
  # maintain the same aspect ratio and keep max dimension at self.max_size_test.
  if max(newh, neww) > 1333:
      pre_scale = 1333 * 1.0 / max(newh, neww)
      newh = newh * pre_scale
      neww = neww * pre_scale
  neww = int(neww + 0.5)
  newh = int(newh + 0.5)

  # Scaling factor for normalized box coordinates scaling in post-processing.
  scaling = max(newh/height, neww/width)
  
  # Padding.
  image = image.resize((neww, newh), resample=Image.Resampling.BILINEAR)
  pad = Image.new("RGB", (1344, 1344))
  pad.paste((0, 0, 0), [0, 0, 1344, 1344])
  pad.paste(image)
  image = np.asarray(pad, dtype=np.float32)

  # Change HWC -> CHW.
  image = np.transpose(image, (2, 0, 1))

  # Change RGB -> BGR.

  #For Danny
  #For batch execution dims of numpy array that will be fed into TRT will be [bs, 3, 1344, 1344].
  #Currently since batch is just one image it is [3, 1344, 1344] == image[[2,1,0]].shape.
  #Hence it just requires preprocessing batch of images and packaging them together into one np array.
  #Scaling in such case will be a list, with unique scaling factor for every image.
  return (scaling, image[[2,1,0]])


class PostProcessor(beam.DoFn):
  """Processes the PredictionResult that consists of
  number of detections per image, box coordinates, scores and classes.

  We loop over all detections to organize attributes on a per
  detection basis. Box coordinates are normalized, hence we have to scale them
  according to original image dimensions. Score is a floating point number
  that provides probability percentage of a particular object. Class is
  an integer that we can transform into actual string class using
  COCO_OBJ_DET_CLASSES as reference.
  """
  def process(self, element: Tuple[str, PredictionResult]) -> Iterable[str]:
    key, prediction_result = element
    filename, scales = key
    num_detections = prediction_result.inference[0]
    boxes = prediction_result.inference[1]
    scores = prediction_result.inference[2]
    classes = prediction_result.inference[3]
    masks = prediction_result.inference[4]
    detections = []

    #Single Batch Implementation
    for i in range(int(num_detections[0])):
      scale = 1344
      scale /= scales
      scale_y = scale
      scale_x = scale
      detections.append({
          'ymin': str(boxes[i][0] * scale_y),
          'xmin': str(boxes[i][1] * scale_x),
          'ymax': str(boxes[i][2] * scale_y),
          'xmax': str(boxes[i][3] * scale_x),
          'score': str(scores[i]),
          'class': int(classes[i]),
          #'mask': masks,
      })

    #For Danny
    #If we are doing batch of multiple images it will be something like this:
    #for i in range(batch_size):
    #    detections.append([])
    #    for n in range(int(nums[i])):
    #        # Select a mask.
    #        mask = masks[i][n]

    #        # Calculate scaling values for bboxes. 
    #        scale = 1344
    #        scale /= scales[i]
    #        scale_y = scale
    #        scale_x = scale
    #        # Append to detections          
    #        detections[i].append({
    #            'ymin': boxes[i][n][0] * scale_y,
    #            'xmin': boxes[i][n][1] * scale_x,
    #            'ymax': boxes[i][n][2] * scale_y,
    #            'xmax': boxes[i][n][3] * scale_x,
    #            'score': scores[i][n],
    #            'class': int(classes[i][n]),
    #            #'mask': mask,
    #        })
    yield filename + ',' + str(detections)


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
      '--engine_path',
      dest='engine_path',
      required=True,
      help='Path to the pre-built Detectron2 Mask R-CNN'
      'TensorRT engine.')
  parser.add_argument(
      '--batch_size',
      dest='batch_size',
      required=True,
      type=int, 
      help='Batch size for TensorRT engine.')
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
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  engine_handler = KeyedModelHandler(
      TensorRTEngineHandlerNumPy(
          min_batch_size=known_args.batch_size,
          max_batch_size=known_args.batch_size,
          engine_path=known_args.engine_path))

  with beam.Pipeline(options=pipeline_options) as p:
    filename_value_pair = (
        p
        | 'ReadImageNames' >> beam.io.ReadFromText(known_args.input)
        | 'ReadImageData' >> beam.Map(
            lambda image_name: read_image(
                image_file_name=image_name, path_to_dir=known_args.images_dir))
        | 'PreprocessImages' >> beam.MapTuple(
            lambda file_name, data: (file_name, preprocess_image(data)))
        | 'AttachScaleToKey' >> beam.Map(attach_scale_to_key))
        
    
    predictions = (
        filename_value_pair
        | 'TensorRTRunInference' >> RunInference(engine_handler)
        | 'ProcessOutput' >> beam.ParDo(PostProcessor()))

    _ = (
        predictions | "WriteOutputToGCS" >> beam.io.WriteToText(
            known_args.output,
            shard_name_template='',
            append_trailing_newlines=True))




if __name__ == '__main__':
  run()

