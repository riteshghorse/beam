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

from typing import List

import proto
from google.api_core.exceptions import NotFound
from google.cloud import aiplatform

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel

__all__ = [
    'VertexAIFeatureStoreEnrichmentHandler',
    'VertexAIFeatureStoreLegacyEnrichmentHandler'
]

_LOGGER = logging.getLogger(__name__)


def _not_found_err_message(
    feature_store_name: str, feature_view_name: str, entity_id: str) -> str:
  """returns a string formatted with given parameters"""
  return (
      "make sure the Feature Store: %s with Feature View "
      "%s has entity_id: %s" %
      (feature_store_name, feature_view_name, entity_id))


class VertexAIFeatureStoreEnrichmentHandler(EnrichmentSourceHandler[beam.Row,
                                                                    beam.Row]):
  """Enrichment handler to interact with Vertex AI Feature Store.

  Use this handler with :class:`apache_beam.transforms.enrichment.Enrichment`
  transform when the Vertex AI Feature Store is set up for
  Bigtable Online serving.

  With the Bigtable Online serving approach, the client fetches all the
  available features for an entity-id. To filter the features to enrich, use
  the `join_fn` param in :class:`apache_beam.transforms.enrichment.Enrichment`.

  **NOTE:** The default severity to report exceptions is logging a warning. For
    this handler, Vertex AI client returns the same exception
    `Requested entity was not found` even though the feature store doesn't
    exist. So make sure the feature store instance exists or set
    `exception_level` as `ExceptionLevel.RAISE`.
  """
  def __init__(
      self,
      project: str,
      location: str,
      api_endpoint: str,
      feature_store_name: str,
      feature_view_name: str,
      row_key: str,
      *,
      exception_level: ExceptionLevel = ExceptionLevel.WARN,
  ):
    """Initializes an instance of `VertexAIFeatureStoreEnrichmentHandler`.

    Args:
      project (str): The GCP project-id for the Vertex AI Feature Store.
      location (str): The region for the Vertex AI Feature Store.
      api_endpoint (str): The API endpoint for the Vertex AI Feature Store.
      feature_store_name (str): The name of the Vertex AI Feature Store.
      feature_view_name (str): The name of the feature view within the
        Feature Store.
      row_key (str): The row key field name containing the unique id
        for the feature values.
      exception_level: a `enum.Enum` value from
        `apache_beam.transforms.enrichment_handlers.utils.ExceptionLevel`
        to set the level when an empty row is returned from the BigTable query.
        Defaults to `ExceptionLevel.WARN`.
    """
    self.project = project
    self.location = location
    self.api_endpoint = api_endpoint
    self.feature_store_name = feature_store_name
    self.feature_view_name = feature_view_name
    self.row_key = row_key
    self.exception_level = exception_level

  def __enter__(self):
    """Connect with the Vertex AI Feature Store."""
    self.client = aiplatform.gapic.FeatureOnlineStoreServiceClient(
        client_options={"api_endpoint": self.api_endpoint})
    self.feature_view_path = self.client.feature_view_path(
        self.project,
        self.location,
        self.feature_store_name,
        self.feature_view_name)

  def __call__(self, request: beam.Row, *args, **kwargs):
    """Fetches feature value for an entity-id from Vertex AI Feature Store.

    Args:
      request: the input `beam.Row` to enrich.
    """
    try:
      entity_id = request._asdict()[self.row_key]
    except KeyError:
      raise ValueError(
          "no entry found for row_key %s in input row" % self.row_key)
    try:
      response = self.client.fetch_feature_values(
          request=aiplatform.gapic.FetchFeatureValuesRequest(
              data_key=aiplatform.gapic.FeatureViewDataKey(key=entity_id),
              feature_view=self.feature_view_path,
              data_format=aiplatform.gapic.FeatureViewDataFormat.PROTO_STRUCT,
          ))
    except NotFound:
      if self.exception_level == ExceptionLevel.WARN:
        _LOGGER.warning(
            _not_found_err_message(
                self.feature_store_name, self.feature_view_name, entity_id))
        return request, beam.Row()
      elif self.exception_level == ExceptionLevel.RAISE:
        raise ValueError(
            _not_found_err_message(
                self.feature_store_name, self.feature_view_name, entity_id))
    response_dict = dict(response.proto_struct)
    return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Clean the instantiated Vertex AI client."""
    self.client = None

  def get_cache_key(self, request: beam.Row) -> str:
    """Returns a string formatted with unique entity-id for the feature values.
    """
    return 'entity_id: %s'


class VertexAIFeatureStoreLegacyEnrichmentHandler(EnrichmentSourceHandler):
  """Enrichment handler to interact with Vertex AI Feature Store (Legacy).

  Use this handler with :class:`apache_beam.transforms.enrichment.Enrichment`
  transform for the Vertex AI Feature Store (Legacy).

  By default, it fetches all the features values for an entity-id. You can
  specify the features names using `feature_ids` to fetch specific features.
  """
  def __init__(
      self,
      project: str,
      location: str,
      api_endpoint: str,
      feature_store_id: str,
      entity_type_id: str,
      feature_ids: List[str],
      row_key: str,
      *,
      exception_level: ExceptionLevel = ExceptionLevel.WARN,
  ):
    """Initializes an instance of `VertexAIFeatureStoreLegacyEnrichmentHandler`.

    Args:
      project (str): The GCP project for the Vertex AI Feature Store (Legacy).
      location (str): The region for the Vertex AI Feature Store (Legacy).
      api_endpoint (str): The API endpoint for the
        Vertex AI Feature Store (Legacy).
      feature_store_id (str): The id of the Vertex AI Feature Store (Legacy).
      entity_type_id (str): The entity type of the feature store.
      feature_ids (List[str]): A list of feature-ids to fetch
        from the Feature Store.
      row_key (str): The row key field name containing the entity id
        for the feature values.
      exception_level: a `enum.Enum` value from
        `apache_beam.transforms.enrichment_handlers.utils.ExceptionLevel`
        to set the level when an empty row is returned from the BigTable query.
        Defaults to `ExceptionLevel.WARN`.
    """
    self.project = project
    self.location = location
    self.api_endpoint = api_endpoint
    self.feature_store_id = feature_store_id
    self.entity_type_id = entity_type_id
    self.feature_ids = feature_ids
    self.row_key = row_key
    self.exception_level = exception_level

  def __enter__(self):
    """Connect with the Vertex AI Feature Store (Legacy)."""
    try:
      _ = aiplatform.Featurestore(
          featurestore_name=self.feature_store_id,
          project=self.project,
          location=self.location,
      )
      self.client = aiplatform.gapic.FeaturestoreOnlineServingServiceClient(
          client_options={'api_endpoint': self.api_endpoint})
      self.entity_type_path = self.client.entity_type_path(
          self.project,
          self.location,
          self.feature_store_id,
          self.entity_type_id)
    except NotFound:
      raise ValueError(
          'Vertex AI Feature Store %s does not exist' % self.feature_store_id)

  def __call__(self, request: beam.Row, *args, **kwargs):
    """Fetches feature value for an entity-id from
    Vertex AI Feature Store (Legacy).

    Args:
      request: the input `beam.Row` to enrich.
    """
    try:
      entity_id = request._asdict()[self.row_key]
    except KeyError:
      raise ValueError(
          "no entry found for row_key %s in input row" % self.row_key)

    try:
      selector = aiplatform.gapic.FeatureSelector(
          id_matcher=aiplatform.gapic.IdMatcher(ids=self.feature_ids))
      response = self.client.read_feature_values(
          request=aiplatform.gapic.ReadFeatureValuesRequest(
              entity_type=self.entity_type_path,
              entity_id=entity_id,
              feature_selector=selector))
    except NotFound:
      raise ValueError(
          _not_found_err_message(
              self.feature_store_id, self.entity_type_id, entity_id))

    response_dict = {}
    proto_to_dict = proto.Message.to_dict(response.entity_view)
    for key, msg in zip(response.header.feature_descriptors,
                        proto_to_dict['data']):
      if msg and 'value' in msg:
        for _, value in msg['value'].items():
          response_dict[key.id] = value
          break  # skip fetching the metadata
      elif self.exception_level == ExceptionLevel.RAISE:
        raise ValueError(
            _not_found_err_message(
                self.feature_store_id, self.entity_type_id, entity_id))
      elif self.exception_level == ExceptionLevel.WARN:
        _LOGGER.warning(
            _not_found_err_message(
                self.feature_store_id, self.entity_type_id, entity_id))
    return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Clean the instantiated Vertex AI client."""
    self.client = None

  def get_cache_key(self, request: beam.Row) -> str:
    """Returns a string formatted with unique entity-id for the feature values.
    """
    return 'entity_id: %s'
