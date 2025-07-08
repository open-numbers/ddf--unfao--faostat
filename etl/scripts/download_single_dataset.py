#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utility script for downloading and testing individual FAO datasets.
This script can be used for debugging and testing purposes.
"""

import json
import os
import os.path as osp
import sys
import argparse
from ddf_utils.factory.common import download


def load_metadata(metadata_file):
    """Load metadata from JSON file"""
    with open(metadata_file, "r", encoding="latin1") as f:
        md = json.load(f)
    return md["Datasets"]["Dataset"]


def find_dataset_by_code(datasets, dataset_code):
    """Find dataset by code"""
    for dataset in datasets:
        if dataset["DatasetCode"] == dataset_code:
            return dataset
    return None


def list_datasets(metadata_file):
    """List all available datasets"""
    datasets = load_metadata(metadata_file)
    print(f"Available datasets ({len(datasets)} total):")
    print("-" * 80)
    for dataset in datasets:
        print(f"Code: {dataset['DatasetCode']}")
        print(f"Name: {dataset['DatasetName']}")
        print(f"Size: {dataset['FileSize']}")
        print(f"URL:  {dataset['FileLocation']}")
        print("-" * 80)


def download_dataset(dataset_code, metadata_file, sources_dir, force=False):
    """Download a specific dataset by code"""
    datasets = load_metadata(metadata_file)
    dataset = find_dataset_by_code(datasets, dataset_code)

    if not dataset:
        print(f"Error: Dataset with code '{dataset_code}' not found")
        return False

    file_location = dataset["FileLocation"]
    filename = file_location.split("/")[-1]
    output_path = osp.join(sources_dir, filename)

    # Check if file already exists
    if osp.exists(output_path) and not force:
        file_size = osp.getsize(output_path)
        print(f"File already exists: {filename} ({file_size} bytes)")
        print("Use --force to overwrite")
        return True

    # Create sources directory if it doesn't exist
    os.makedirs(sources_dir, exist_ok=True)

    print(f"Downloading {dataset_code}: {dataset['DatasetName']}")
    print(f"File: {filename}")
    print(f"Size: {dataset['FileSize']}")
    print(f"URL: {file_location}")

    try:
        download(file_location, output_path)
        print(f"Successfully downloaded to: {output_path}")
        return True
    except Exception as e:
        print(f"Failed to download: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Download and test individual FAO datasets"
    )
    parser.add_argument(
        "--metadata",
        default="../source/datasets_E.json",
        help="Path to metadata JSON file",
    )
    parser.add_argument(
        "--output-dir", default="../datasets", help="Path to sources directory"
    )
    parser.add_argument(
        "--list", action="store_true", help="List all available datasets"
    )
    parser.add_argument(
        "--download", metavar="DATASET_CODE", help="Download specific dataset by code"
    )
    parser.add_argument(
        "--force", action="store_true", help="Force download even if file exists"
    )

    args = parser.parse_args()

    if args.list:
        list_datasets(args.metadata)
    elif args.download:
        success = download_dataset(
            args.download, args.metadata, args.output_dir, args.force
        )
        sys.exit(0 if success else 1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
