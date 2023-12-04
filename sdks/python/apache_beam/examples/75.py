import logging

import apache_beam as beam
from apache_beam import CoGroupByKey
from apache_beam import Create
from apache_beam import Map


def run(argv=None):
  # Define the input key-value pairs.
  jobs = [("Anna", "SWE"), ("Kim", "Data Engineer"), ("Kim", "Data Scientist"),
          ("Robert", "Artist"), ("Sophia", "CEO")]
  hobbies = [("Anna", "Painting"), ("Kim", "Football"), ("Kim", "Gardening"),
             ("Robert", "Swimming"), ("Sophia", "Mathematics"),
             ("Sophia", "Tennis")]

  # Create two PCollections with beam.Create().
  # Use beam.CoGroupByKey() to group elements from these two PCollections
  # by their key.
  with beam.Pipeline() as p:
    jobs_pcol = p | "Create Jobs" >> Create(jobs)
    hobbies_pcol = p | "Create Hobbies" >> Create(hobbies)
    _ = ((jobs_pcol, hobbies_pcol) | "CoGroupByKey" >> CoGroupByKey()
         | "Log" >> Map(logging.info))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
