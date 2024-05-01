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
from typing import Any
from typing import Callable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union

from apache_beam.pvalue import Row
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

QueryFn = Callable[[beam.Row], str]


class BigQueryEnrichmentHandler(EnrichmentSourceHandler[Union[Row, List[Row]],
                                                        Union[Row, List[Row]]]):
  def __init__(
      self,
      project: str,
      query_template: str,
      fields: List[str],
      *,
      query_fn: Optional[QueryFn] = None,
      condition_template: Optional[str] = "",
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
  ):
    """Initialize the `BigQueryEnrichmentHandler`.

    Args:
      project: GCP project ID for the BigQuery table.
      query_template: a raw query template to use for fetching data.
        It must contain the placeholder `{}` to replace it with `fields` to
        fetch values specific to `fields`.
      fields: Fields present in the input row to substitute in the placeholders
        in query_template.
    """
    self.project = project
    self.query_template = query_template
    self.fields = fields
    self.query_fn = query_fn
    self.condition_template = condition_template
    self._batching_kwargs = {}
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size

  def __enter__(self):
    self.client = bigquery.Client(project=self.project)

  def __call__(self, request: Union[beam.Row, List[beam.Row]], *args, **kwargs):
    if isinstance(request, beam.Row):
      if self.query_fn:
        # if a query_fn is provided then it return a list of values
        # that should be populated into the query template string.
        values = self.query_fn(request)
      else:
        request_dict = request._asdict()
        values = list(map(request_dict.get, self.fields))
      # construct the query.
      query = self.query_template.format(*values)
      try:
        # make a call to bigquery. Probably use QueryParameters to prevent
        # attacks like SQL injection.
        result = self.client.query(query=query).result()
        response_dict = {}
        for r in result:
          response_dict = dict(zip(r.keys(), r.values()))
      except Exception:
        raise RuntimeError
      return request, beam.Row(**response_dict)
    else:
      # handle the batching case here.
      batch_size = len(request)
      raw_query = self.query_template
      # requests_map = {}
      if batch_size > 1:
        batched_condition_template = ' or '.join([self.condition_template] *
                                                 batch_size)
        raw_query = self.query_template.replace(
            self.condition_template, batched_condition_template)
      values = []
      responses = []
      for req in request:
        request_dict = req._asdict()
        current_values = [request_dict.get(field) for field in self.fields]
        values.extend(current_values)
      query = raw_query.format(*values)
      try:
        # make a call to bigquery. Probably use QueryParameters to prevent
        # attacks like SQL injection.
        result = self.client.query(query=query).result()
        for r in result:
          response_dict = dict(zip(r.keys(), r.values()))
          responses.append((request[0], beam.Row(**response_dict)))
      except Exception as e:
        raise RuntimeError(e)
      return responses

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()

  def get_cache_key(self, request: Union[beam.Row, List[beam.Row]]) -> str:
    return 'primary_key'

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    """Returns a kwargs suitable for `beam.BatchElements`."""
    return self._batching_kwargs
