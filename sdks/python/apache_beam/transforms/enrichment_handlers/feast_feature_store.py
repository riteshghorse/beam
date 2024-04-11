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
import tempfile
from pathlib import Path
from typing import Optional, List

from feast import FeatureStore

import apache_beam as beam
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel

__all__ = [
    'FeastFeatureStoreEnrichmentHandler',
]

_LOGGER = logging.getLogger(__name__)

LOCAL_FEATURE_STORE_YAML_FILENAME = 'fs_yaml_file.yaml'


def download_fs_yaml_file(gcs_fs_yaml_file: str):
  fs = GCSFileSystem(pipeline_options={})
  with fs.open(gcs_fs_yaml_file, 'r') as gcs_file:
    with tempfile.NamedTemporaryFile(suffix=LOCAL_FEATURE_STORE_YAML_FILENAME,
                                     delete=False) as local_file:
      local_file.write(gcs_file.read())
      return Path(local_file.name)


def _validate_feature_names(feature_names, feature_service_name):
  if not bool(feature_names or feature_service_name):
    raise ValueError(
        'Please provide either a list of feature names to fetch '
        'from online store or a feature service name for the '
        'online store!')


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
      entity_id: str,
      feature_store_yaml_path: Optional[str] = "",
      feature_names: List[str] = None,
      feature_service_name: str = "",
      full_feature_names: bool = False,
      *,
      exception_level: ExceptionLevel = ExceptionLevel.WARN,
      **kwargs,
  ):
    """Initializes an instance of `FeastFeatureStoreEnrichmentHandler`.

    Args:
      pass
    """
    self.entity_id = entity_id
    self.feature_store_yaml_path = feature_store_yaml_path
    self.feature_names = feature_names
    self.feature_service_name = feature_service_name
    self.full_feature_names = full_feature_names
    self._exception_level = exception_level
    self._kwargs = kwargs if kwargs else {}
    _validate_feature_names(self.feature_names, self.feature_service_name)

  def __enter__(self):
    """Connect with the Feast Feature Store."""
    local_repo_path = download_fs_yaml_file(self.feature_store_yaml_path)
    self.store = FeatureStore(fs_yaml_file=local_repo_path)
    if self.feature_service_name:
      self.features = self.store.get_feature_service(self.feature_service_name)
    else:
      self.features = self.feature_names

  def __call__(self, request: beam.Row, *args, **kwargs):
    """Fetches feature values for an entity-id from the Feast Feature Store.

    Args:
      request: the input `beam.Row` to enrich.
    """
    request_dict = request._asdict()
    response_dict = self.store.get_online_features(
        features=self.features,
        entity_rows=[{
            self.entity_id: request_dict[self.entity_id]
        }],
        full_feature_names=self.full_feature_names).to_dict()
    return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Clean the instantiated Feast feature store client."""
    self.store = None

  def get_cache_key(self, request: beam.Row) -> str:
    """Returns a string formatted with unique entity-id for the feature values.
    """
    return 'entity_id: %s' % request._asdict()[self.entity_id]
