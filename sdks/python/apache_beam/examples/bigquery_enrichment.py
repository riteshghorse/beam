import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.bigquery import BigQueryEnrichmentHandler


def run(argv=None, save_main_session=True):
  requests = [
      beam.Row(
          id='13842',
          name='low profile dyed cotton twill cap - navy w39s55d',
          quantity=2),
      beam.Row(
          id='15816',
          name='low profile dyed cotton twill cap - putty w39s55d',
          quantity=1),
  ]

  query_template = """
    SELECT cast(id as string) AS id,
      lower(name) as name,
      lower(category) as category,
      cost,
      retail_price
    FROM bigquery-public-data.thelook_ecommerce.products
    WHERE cast(id as string)="{}" AND lower(name)="{}";
    """

  fields = ['id', 'name']

  handler = BigQueryEnrichmentHandler(
      project='google.com:clouddfe',
      query_template=query_template,
      fields=fields)

  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:
    _ = (
        p
        | "Create Data" >> beam.Create(requests)
        | "Enrichment" >> Enrichment(handler)
        | "Output" >> beam.Map(lambda x: print(x)))


if __name__ == '__main__':
  run()
