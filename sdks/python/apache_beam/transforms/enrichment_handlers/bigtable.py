from typing import List

from google.cloud import bigtable

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler


class EnrichWithBigTable(EnrichmentSourceHandler[dict, beam.Row]):
  """

  Args:
    params for configuring bigtable
    list of column names
    list of column family ids
  """
  def __init__(
      self,
      project_id: str,
      instance_id: str,
      table_id: str,
      row_key: str,
      column_family_ids: List[str],
      column_ids: List[str]):
    self._project_id = project_id
    self._instance_id = instance_id
    self._table_id = table_id
    self._row_key = row_key
    self._column_family_ids = column_family_ids
    self._column_ids = column_ids

  def __enter__(self):
    client = bigtable.Client(project=self._project_id)
    instance = client.instance(self._instance_id)
    self._table = instance.table(self._table_id)

  def __call__(self, request: dict, *args, **kwargs):
    row_key = request[self._row_key].encode()
    row = self._table.read_row(row_key)
    if self._column_family_ids and self._column_ids:
      response_dict = {}
      for column_family_id in self._column_family_ids:
        response_dict[column_family_id] = {}
        for column_id in self._column_ids:
          response_dict[column_family_id][column_id] = row.cells[
              column_family_id][column_id.encode()][0].value.decode('utf-8')
      yield (request, response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    self._table = None
