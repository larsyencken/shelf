# shelf

_A personal ETL and data lake._

Status: in alpha, changing often

## Overview

Shelf is an opinionated small-scale ETL framework for managing data files and directories in a content-addressable way.

## Core principles

- **A reusable framework.** Shelf provides a structured way of managing data files, scripts and their interdependencies that can be used across multiple projects.
- **First class metadata.** Every data file has an accompanying metadata sidecar that can be used to store provenance, licensing and other information.
- **Content addressed.** A `shelf` DAG is a Merkle tree of checksums that includes data, metadata and scripts, used to lazily rebuild only what is out of date.
- **Data versioning.** Every step in the DAG has a URI that includes a version, which can be an ISO date or `latest`, to encourage a reproducible workflow that still allows for change.
- **Opinionated workflow.** Shelf encourages an opinionated workflow for data, that encourages you to create tables that are clean, harmonized and remixable, and ultimately let you treat your Shelf as a reliable catalog of data for doing data science work from.
- **Polyglot.** Shelf is a Python framework, but allows you to write step definitions in the tool of your choice, as long as they meet a simple signature.

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

### Building your shelf

Run `shelf run` to fetch any data that's out of date, and build any derived tables.

## Bugs

Please report any issues at: https://github.com/larsyencken/shelf/issues

## Changelog

- `dev`
  - Initialise a repo with `shelf.yaml`
  - `shelf add` and `shelf run` with file and directory support
  - Only fetch things that are out of date
  - `shelf list` to see what datasets are available
  - `shelf audit` to ensure your shelf is coherent and correct
