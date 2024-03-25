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
import logging
from pathlib import Path
from typing import Optional

from feast import FeatureStore
from feast.repo_config import load_repo_config

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel

__all__ = [
    'FeastFeatureStoreEnrichmentHandler',
]

_LOGGER = logging.getLogger(__name__)


class FeastFeatureStoreEnrichmentHandler(EnrichmentSourceHandler[beam.Row,
                                                                 beam.Row]):
  """Enrichment handler to interact with the Feast Feature Store.

  Use this handler with :class:`apache_beam.transforms.enrichment.Enrichment`
  transform.

  To filter the features to enrich, use the `join_fn` param in
  :class:`apache_beam.transforms.enrichment.Enrichment`.
  """
  def __init__(
      self,
      *,
      repo_path: Optional[str] = "",
      feature_store_yaml_file: Optional[Path] = None,
      exception_level: ExceptionLevel = ExceptionLevel.WARN,
      **kwargs,
  ):
    """Initializes an instance of `FeastFeatureStoreEnrichmentHandler`.

    Args:
      pass
    """
    self.repo_path = repo_path
    self.feature_store_yaml_file = feature_store_yaml_file
    self._exception_level = exception_level
    self._kwargs = kwargs if kwargs else {}
    self.repo_config = load_repo_config(
        self.repo_path, self.feature_store_yaml_file)

  def __enter__(self):
    """Connect with the Feast Feature Store."""
    self.store = FeatureStore(config=self.repo_config)

  def __call__(self, request: beam.Row, *args, **kwargs):
    """Fetches feature values for an entity-id from the Feast Feature Store.

    Args:
      request: the input `beam.Row` to enrich.
    """
    response_dict = {}
    # use method `get_online_features`
    return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Clean the instantiated Feast feature store client."""
    pass

  def get_cache_key(self, request: beam.Row) -> str:
    """Returns a string formatted with unique entity-id for the feature values.
    """
    return 'entity_id: %s' % request._asdict()[self.row_key]
