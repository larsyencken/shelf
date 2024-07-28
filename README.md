# shelf

_A personal ETL and data lake._

## Overview

Shelf is a library and personal ETL framework for managing small data files and directories in a content-addressable way.

Metadata is kept on-disk in a `data/` folder. Data is definitively stored in an S3-compatible store, but fetched locally for processing.

```mermaid
subgraph git-repo
    metadata
end

subgraph s3
    data
end

metadata --> data
```

Every dataset in a shelf is an immutable file or folder with a corresponding metadata file containing its checksum. A dataset is identified by a path, which must end in a `version` (a date or `latest`).

For example, valid dataset paths include:

- `countries/2020-04-07`
- `who/covid-19/latest`
- `some/very/long/qualified/path/2024-07-01`

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

From within your shelf folder, run `shelf add path/to/your/file_or_folder dataset_name` to add a file to your shelf. See the earlier overview for choosing a dataset name.

```
shelf add ~/Downloads/countries.csv countries/latest
```

This will upload the file to your S3-compatible storage, and create a metadata file at `data/<dataset_name>.meta.yaml` directory for you to complete.

The metadata format has some minimum fields, but is meant for you to extend as needed for your own purposes. Best practice would be to retain the provenance and licence information of any data you add to your shelf, especially if it originates from a third party.

### Restoring a file from the shelf

Your shelf is designed to be managed in a git repository, with data stored outside of the repo in S3. This means that on a new machine, you can clone the repo, and run `shelf get` to restore all the data from the shelf.

You can also run `shelf get some_regex` to only fetch files whose dataset name matches the regex.

## Bugs

Please report any issues at: https://github.com/larsyencken/shelf/issues

## Changelog

- `dev``
  - Initialise a repo with `shelf.yaml`
  - `shelf add` and `shelf get` with file and directory support
