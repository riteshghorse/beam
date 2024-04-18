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
from typing import Callable, Optional

from google.cloud import bigquery

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.typehints import List

QueryFn = Callable[[beam.Row], str]


class BigQueryEnrichmentHandler(EnrichmentSourceHandler):
  def __init__(
      self,
      project: str,
      query_template: Optional[str] = "",
      fields: Optional[List[str]] = None,
      query_fn: Optional[QueryFn] = None,
  ):
    """Initialize the `BigQueryEnrichmentHandler`.

    Args:
      project: GCP project ID for the BigQuery table.
      query_template: a raw query template to use for fetching data.
        It must contain the placeholder `{}` to replace it with `fields` to
        fetch values specific to `fields`.
      fields: Fields present in the input row to substitue in the placeholders
        in query_template.
    """
    self.project = project
    self.query_template = query_template
    self.fields = fields
    self.query_fn = query_fn

  def __enter__(self):
    self.client = bigquery.Client(project=self.project)

  def __call__(self, request: beam.Row, *args, **kwargs):
    if self.query_fn:
      query = self.query_fn(request)
    else:
      request_dict = request._asdict()
      values = list(map(request_dict.get, self.fields))
      query = self.query_template.format(*values)
      if None in values:
        raise ValueError
    try:
      result = self.client.query(query=query).result()
      response_dict = {}
      for r in result:
        response_dict = dict(zip(r.keys(), r.values()))
    except Exception:
      raise RuntimeError
    return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()

  def get_cache_key(self, request: beam.Row) -> str:
    # for testing, just use the first field for caching
    return request._asdict()[self.fields[0]]
