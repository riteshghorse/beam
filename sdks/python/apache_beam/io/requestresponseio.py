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

from abc import ABC, abstractmethod
from typing import Generic, NamedTuple, TypeVar

import apache_beam as beam
from apache_beam.transforms import PTransform

RequestT = TypeVar('RequestT')
ResponseT = TypeVar('ResponseT')


class Result(NamedTuple('Result', [('request', RequestT),
                                   ('response', ResponseT)])):
  __slots__ = ()

  def __new__(cls, request, response):
    return super().__new__(cls, request, response)


Result.__doc__ = """A NamedTuple containing both request and response
  from the API"""
Result.request.__doc__ = """The request to the API."""
Result.response.__doc__ = """The response from the API."""


class Caller(ABC, Generic[RequestT, ResponseT]):
  """Caller abstract base class for user custom code intended for API calls.
  """
  @abstractmethod
  def call(self, request: RequestT) -> ResponseT:
    """
    Calls a Web API with the given request and returns a response.
    
    Args:
      request: (RequestT) The request to be made to the Web API.
    Returns:
      A response of type ResponseT.
    """
    pass


class SetupTeardown(ABC):
  """SetupTeardown abstract base class for user provided code and called in
  setup and teardown lifecycle methods."""
  @abstractmethod
  def setup(self):
    """setup called during the DoFn's setup lifecycle method."""
    pass

  @abstractmethod
  def teardown(self):
    """teardown called during the DoFn's teardown lifecycle method."""
    pass


class RequestResponse(PTransform[beam.PCollection[RequestT], Result[ResponseT]],
                      Generic[RequestT, ResponseT]):
  def __init__(self, caller: Caller):
    self._caller = caller

  def annotations(self):
    return {
        'model_handler': str(self._model_handler),
        'model_handler_type': (
            f'{self._model_handler.__class__.__module__}'
            f'.{self._model_handler.__class__.__qualname__}'),
        **super().annotations()
    }

  def expand(self,
             input: beam.PCollection[RequestT]) -> beam.PCollection[ResponseT]:
    return Result(input, None)


class CallShouldBackoff(ABC):
  """Informs whether the call to an API should backoff."""
  @abstractmethod
  def updateFromException(self, exception: Exception):
    """Update the state of whether to backoff from the exception."""
    pass

  @abstractmethod
  def updateFromResponse(self, response: ResponseT):
    """Update the state of whether to backoff from the response."""
    pass

  @abstractmethod
  def value(self) -> bool:
    """Report whether to backoff."""
    pass


class CallShouldBackoffBasedOnRejectionProbability(CallShouldBackoff):
  def __init__(
      self,
      multiplier: float = 2.0,
      threshold: float = None,
      n_requests: int = 0,
      n_accepts: int = 0,
  ) -> None:
    self.multiplier = multiplier
    self.threshold = threshold
    self.n_requests = n_requests
    self.n_accepts = n_accepts
