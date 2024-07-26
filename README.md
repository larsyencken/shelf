# shelf

A personal ETL and data lake.

## Overview

Shelf is a personal ETL framework for managing small data files and directories in a content-addressable way.

## Shelving a Data File

To shelve a data file by adding it in a content-addressable way to the S3-compatible store, use the `shelf add` function.

### Usage

1. Ensure you have configured your S3-compatible storage credentials in a `.env` file. You can use the provided `.env.example` as a template.

2. Call the `shelf add` function with the appropriate arguments:

```python
from shelf import shelve_data_file

file_path = 'path/to/your/datafile.csv'
path = 'your_namespace/your_dataset/your_version'

shelve_data_file(file_path, path)
```

This will:

- Generate a checksum for the file.
- Upload the file to the S3-compatible store using the checksum as the key.
- Create a metadata record in the `metadata` directory with the checksum and other details.
- Open the metadata file in an interactive editor for editing.

## Shelving a Directory

To shelve a directory by adding all its files in a content-addressable way to the S3-compatible store, use the `shelf add` function.

### Usage

1. Ensure you have configured your S3-compatible storage credentials in a `.env` file. You can use the provided `.env.example` as a template.

2. Call the `shelf add` function with the appropriate arguments:

```python
from shelf import shelve_data_file

directory_path = 'path/to/your/directory'
path = 'your_namespace/your_dataset/your_version'

shelve_data_file(directory_path, path)
```

This will:

- Generate a checksum for each file in the directory.
- Upload each file to the S3-compatible store using the checksum as the key.
- Create a manifest file containing the directory listing and shelve it.
- Create a metadata record in the `metadata` directory indicating that it's a directory being unpacked.
- Open the metadata file in an interactive editor for editing.

## Command Line Usage

You can also use the `shelf` command from the command line to shelve a data file or directory.

### Usage

1. Ensure you have configured your S3-compatible storage credentials in a `.env` file. You can use the provided `.env.example` as a template.

2. Run the `shelf add` command with the appropriate arguments:

```sh
shelf add path/to/your/datafile.csv your_namespace/your_dataset
```

or

```sh
shelf add path/to/your/datafile.csv your_namespace/your_dataset/your_version
```

or

```sh
shelf add path/to/your/directory your_namespace/your_dataset/your_version
```

This will:

- Generate a checksum for the file or each file in the directory.
- Upload the file or each file in the directory to the S3-compatible store using the checksum as the key.
- Create a manifest file containing the directory listing and shelve it (if a directory is provided).
- Create a metadata record in the `metadata` directory with the checksum and other details.
- Print a message indicating the file or directory is being shelved.
- Open the metadata file in an interactive editor for editing.
- Copy the file or directory to the `data` directory with the same structure as the metadata directory, keeping its original extension.

## Restoring Data

To restore data from the content store and unpack it into the `data/` folder, use the `shelf get` command.

### Usage

1. Ensure you have configured your S3-compatible storage credentials in a `.env` file. You can use the provided `.env.example` as a template.

2. Run the `shelf get` command with the appropriate arguments:

```sh
shelf get [optional_regex]
```

This will:

- Fetch things from the content store and unpack them into `data/`.
- If an optional regex argument is provided, it will match against metadata path names to determine what to include.
- If no argument is provided, it will walk the `metadata/` folder and fetch everything.
- Only re-fetch things whose local checksum in the `data/` folder is out of date.
- Restore directories (and their subdirectories) from the content store.
