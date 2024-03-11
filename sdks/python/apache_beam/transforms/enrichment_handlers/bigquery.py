from google.cloud import bigquery

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.typehints import List


class BigQueryEnrichmentHandler(EnrichmentSourceHandler):
  def __init__(
      self,
      project: str,
      query_template: str,
      fields: List[str],
  ):
    """Initialize the `BigQueryEnrichmentHandler`.

    Args:
      project: GCP project id for the BigQuery table.
      query_template: a raw query template to use for fetching data.
        It must contain the placeholder `{}` to replace it with `fields` to
        fetch values specific to `fields`.
      fields: Fields present in the input row to substitue in the placeholders
        in query_template.
    """
    self.project = project
    self.query_template = query_template
    self.fields = fields

  def __enter__(self):
    self.client = bigquery.Client(project=self.project)

  def __call__(self, request: beam.Row, *args, **kwargs):
    request_dict = request._asdict()
    values = list(map(request_dict.get, self.fields))
    if None in values:
      raise ValueError
    try:
      query = self.query_template.format(*values)
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
