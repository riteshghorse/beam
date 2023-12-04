from typing import TypeVar, Iterable, Callable, Optional

import apache_beam as beam
from apache_beam.io.requestresponseio import Caller
from apache_beam.io.requestresponseio import CacheReader
from apache_beam.io.requestresponseio import CacheWriter
from apache_beam.io.requestresponseio import DEFAULT_TIMEOUT
from apache_beam.io.requestresponseio import PreCallThrottler
from apache_beam.io.requestresponseio import Repeater
from apache_beam.io.requestresponseio import ShouldBackOff

InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')


def cross_join(left, right):
  left_dict = left.as_dict()
  right_key_data = right[left['enrichment_key']]
  for field in right_key_data.values():
    left_dict[field] = right_key_data[field]
  yield beam.Row(**left_dict)


class EnrichmentSourceHandler(Caller):
  pass


class HTTPSourceHandler(EnrichmentSourceHandler):
  def __init__(self, url):
    self._url = url

  def __call__(self, *args, **kwargs):
    pass


class Enrichment(beam.PTransform[beam.PCollection[InputT],
                                 beam.PCollection[OutputT]]):
  def __init__(
      self,
      source_handler: EnrichmentSourceHandler,
      enrichment_key: str,
      enrichment_fields: Iterable[str],
      enrichment_fn: Callable = None,
      timeout: Optional[float] = DEFAULT_TIMEOUT,
      should_backoff: Optional[ShouldBackOff] = None,
      repeater: Optional[Repeater] = None,
      cache_reader: Optional[CacheReader] = None,
      cache_writer: Optional[CacheWriter] = None,
      throttler: Optional[PreCallThrottler] = None):
    self._source_handler = source_handler
    self._enrichment_key = enrichment_key
    self._enrichment_fields = enrichment_fields
    self._enrichment_fn = enrichment_fn
    self._timeout = timeout
    self._should_backoff = should_backoff
    self._repeater = repeater
    self._cache_reader = cache_reader
    self._cache_writer = cache_writer
    self._throttler = throttler

  def expand(self, input_row: InputT) -> OutputT:
    pass
