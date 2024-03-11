---
title: "Custom Enrichment Handler"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Custom Enrichment Handler

This page describes how to write a custom enrichment handler for [enrichment transform](documentation/transforms/python/elementwise/enrichment.md).
Custom enrichment handler allows you to do real-time key-value lookup from a external service within a pipeline.
A custom handler is useful when your use-case interacts with a service whose handler is not provided with beam package
or when you want to customize the behavior of an existing handler in beam package.

An enrichment handler implements the [`EnrichmentSourceHandler`](link) class.
There are three essential components when writing an enrichment handler:
1. Setting up a client - implement the `__enter__` method.
2. Making a call to the external service - implement the `__call__` method. 
3. Tear down a client - implement the `__exit__` method.

For example, consider you want to write an enrichment handler for interacting with BigQuery.

{{< highlight py >}}

class BigQueryEnrichmentHandler(EnrichmentSourceHandler)

{{< /highlight >}}
