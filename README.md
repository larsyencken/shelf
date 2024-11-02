# shelf

[![CI](https://github.com/larsyencken/shelf/actions/workflows/ci.yml/badge.svg)](https://github.com/larsyencken/shelf/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)

_A personal ETL and data lake._

Status: in alpha, changing often

## Overview

Shelf is an opinionated small-scale ETL framework for managing data files and directories in a content-addressable way.

## Core principles

- **A reusable framework.** Shelf provides a structured way of managing data files, scripts and their interdependencies that can be used across multiple projects.
- **First class metadata.** Every data file has an accompanying metadata sidecar that can be used to store provenance, licensing and other information.
- **Content addressed.** A `shelf` DAG is a Merkle tree of checksums that includes data, metadata and scripts, used to lazily rebuild only what is out of date.
- **Data versioning.** Every step in the DAG has a URI that includes a version, which can be an ISO date or `latest`, to encourage a reproducible workflow that still allows for change.
- **SQL support.** Shelf is a Python framework, but allows you to write steps in SQL which will be executed by DuckDB.
- **Parquet interchange.** All derived tables are generated as Parquet, which makes reuse easier.

## Usage

### Install the package

Start by installing the shelf package, either globally, or into an existing Python project.

`pip install git+https://github.com/larsyencken/shelf`

### Initialise a shelf

Enter the folder where you want to store your data and metadata, and run:

`shelf init`

This will create a `shelf.yaml` file, which will serve as the catalogue of all the data in your shelf.

### Configure object storage

You will need to configure your S3-compatible storage credentials in a `.env` file, in the same directory as your `shelf.yaml` file. Define:

```
S3_ACCESS_KEY=your_application_key_id
S3_SECRET_KEY=your_application_key
S3_BUCKET_NAME=your_bucket_name
S3_ENDPOINT_URL=your_endpoint_url
```

Now your shelf is ready to use.

### Shelving a file or folder

From within your shelf folder, run `shelf snapshot path/to/your/file_or_folder dataset_name` to add a file to your shelf. See the earlier overview for choosing a dataset name.

```
shelf snapshot ~/Downloads/countries.csv countries/latest
```

This will upload the file to your S3-compatible storage, and create a metadata file at `data/<dataset_name>.meta.yaml` directory for you to complete.

The metadata format has some minimum fields, but is meant for you to extend as needed for your own purposes. Best practice would be to retain the provenance and licence information of any data you add to your shelf, especially if it originates from a third party.

### Creating a new table

To create a new table, use the `shelf new-table <table-path> [dep1 [dep2 [...]]` command. This command will create a placeholder executable script that generates an example data file of the given type based on the file extension (.parquet or .sql).

For example, to create a new table with a Parquet placeholder script:

```
shelf new-table path/to/your/table
```

This will create a placeholder script that generates an example Parquet file with the following content:

```
#!/usr/bin/env python3
import sys
import polars as pl

data = {
    "a": [1, 1, 3],
    "b": [2, 3, 5],
    "c": [3, 4, 6]
}

df = pl.DataFrame(data)

output_file = sys.argv[-1]
df.write_parquet(output_file)
```

For example, to create a new table with a SQL placeholder script:

```
shelf new-table path/to/your/table.sql
```

This will create a placeholder script that generates an example SQL file with the following content:

```
-- SQL script to create a table
CREATE TABLE example_table AS
SELECT
    1 AS a,
    2 AS b,
    3 AS c
```

The command also supports the `--edit` option to open the metadata file for the table in your editor:

```
shelf new-table path/to/your/table --edit
```

### Executing SQL step definitions

If a `.sql` step definition is detected, it will be executed using DuckDB with an in-memory database. The SQL file can use `{variable}` to interpolate template variables. The following template variables are available:

- `{output_file}`: The path to the output file.
- `{dependency}`: The path of each dependency, simplified to a semantic name.

### Building your shelf

Run `shelf run` to fetch any data that's out of date, and build any derived tables.

## Bugs

Please report any issues at: https://github.com/larsyencken/shelf/issues

## Changelog

- `dev`
  - Initialise a repo with `shelf.yaml`
  - `shelf snapshot` and `shelf run` with file and directory support
  - Only fetch things that are out of date
  - `shelf list` to see what datasets are available
  - `shelf audit` to ensure your shelf is coherent and correct
  - `shelf db` to enter an interactive DuckDB shell with all your data
