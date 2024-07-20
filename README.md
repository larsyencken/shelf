# shelf
A personal ETL and data lake.

## Shelving a Data File

To shelve a data file by adding it in a content-addressable way to the S3-compatible store, use the `shelve_data_file` function.

### Usage

1. Ensure you have configured your S3-compatible storage credentials in a `.env` file. You can use the provided `.env.example` as a template.

2. Call the `shelve_data_file` function with the appropriate arguments:

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

## Command Line Usage

You can also use the `shelve` command from the command line to shelve a data file.

### Usage

1. Ensure you have configured your S3-compatible storage credentials in a `.env` file. You can use the provided `.env.example` as a template.

2. Run the `shelve` command with the appropriate arguments:

```sh
shelve path/to/your/datafile.csv your_namespace/your_dataset
```

or

```sh
shelve path/to/your/datafile.csv your_namespace/your_dataset/your_version
```

This will:
- Generate a checksum for the file.
- Upload the file to the S3-compatible store using the checksum as the key.
- Create a metadata record in the `metadata` directory with the checksum and other details.
- Copy the file to the `data` directory with the same structure as the metadata directory, keeping its original extension.
