import logging

import apache_beam as beam
from apache_beam import Create
from apache_beam import GroupByKey
from apache_beam import Map


def run(argv=None):
  # Define the input key-value pairs
  elements = [("Mammal", "Dog"), ("Mammal", "Cat"), ("Fish", "Salmon"),
              ("Amphibian", "Snake"), ("Bird", "Eagle"), ("Bird", "Owl"),
              ("Mammal", "Algo")]

  # Create a PCollection of elements defined above with beam.Create().
  # Use beam.GroupByKey() to group elements by their key.
  with beam.Pipeline() as p:
    _ = (
        p | "Create Elements" >> Create(elements)
        | "Group Elements" >> GroupByKey()
        | "Log" >> Map(logging.info))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
