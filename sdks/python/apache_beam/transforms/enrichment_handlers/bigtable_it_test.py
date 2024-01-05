import typing
import unittest

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.bigtable import EnrichWithBigTable


class _Currency(typing.NamedTuple):
  s_id: int
  id: str


class TestBigTableEnrichment(unittest.TestCase):
  def test_enrichment_with_bigtable(self):
    # req = _Currency(s_id=1, id='usd')
    req = {'s_id': 1, 'id': 'usd'}
    column_family_ids = ['test-column']
    column_ids = ['id', 'value']
    bigtable = EnrichWithBigTable(
        'google.com:clouddfe',
        'beam-test',
        'riteshghorse-test',
        'id',
        column_family_ids,
        column_ids)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create([req])
          | "Enrich W/ BigTable" >> Enrichment(bigtable)
          | 'Write' >> WriteToText('enrich.txt'))


if __name__ == '__main__':
  unittest.main()
