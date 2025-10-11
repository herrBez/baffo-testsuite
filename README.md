# baffo-testsuite

A comprehensive test suite for the [baffo repository](https://github.com/herrBez/baffo). Its goal is to verify that Logstash pipelines and their translated Elasticsearch ingest pipelines produce semantically equivalent results when processing sample data. While the suite can detect cases where the pipelines are not equivalent, it cannot guarantee complete equivalence in all scenarios.

## Overview

This project automates the deployment of an Elasticsearch cluster and configures Logstash with Central Pipeline Management. It streamlines the process of testing data ingestion and pipeline translation.

## How It Works

1. **Cluster Setup**: Spin up an Elasticsearch cluster using Docker Compose.
2. **Pipeline Configuration**: Configure Logstash pipelines centrally.
3. **Test Execution**: For each test case, use the provided documents (`docs.ndjson`) and pipeline configuration (`pipeline.conf`).
4. **Pipeline Translation**: Convert Logstash pipelines to Elasticsearch ingest pipelines using `baffo`.
5. **Pipeline Deployment**: Upload both the original and translated pipelines to Elasticsearch.
6. **Data Ingestion**: Feed documents into Elasticsearch through both pipelines.
7. **Verification**: Compare the indexed documents to ensure both pipelines produce semantically equivalent results, ignoring certain system-generated fields.

## Folder Structure

- `docs.ndjson`: Sample documents for testing.
- `pipeline.conf`: Logstash pipeline configuration.
- `pipelines.json`: Translated ingest pipelines.

## Purpose

The primary purpose of this suite is to help developers verify that Logstash pipelines and their translated Elasticsearch ingest pipelines process data in a semantically equivalent way. By automating the comparison of results from both pipeline types, the suite makes it easier to detect discrepancies, confirm correct behavior, and identify potential regressions when pipeline configurations or the `baffo` translation tool are changed or updated or even when Logstash and Elasticsearch are updated.


