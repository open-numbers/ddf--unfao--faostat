# -*- coding: utf-8 -*-

import json
import os
import os.path as osp
import pandas as pd
from ddf_utils.factory.common import download


metadata_url = "https://bulks-faostat.fao.org/production/datasets_E.json"
metadata_out_file = "../source/datasets_E.json"
dataset_list_file = "../source/dataset_list.csv"
sources_dir = "../datasets"


def should_skip_dataset(dataset_name, file_location):
    """Check if a dataset should be skipped based on known criteria"""
    # Check dataset name patterns (case insensitive)
    dataset_name_lower = dataset_name.lower()
    skip_name_patterns = ["archives", "monthly", "survey", "security"]

    for pattern in skip_name_patterns:
        if pattern in dataset_name_lower:
            return True

    # Check hardcoded filenames
    filename = file_location.split("/")[-1]
    skip_files = [
        "Producción_Cultivos_S_Todos_los_Datos.zip",  # not in English
        "Employment_Indicators_E_All_Data_(Normalized).zip",  # different layout
        "Food_Aid_Shipments_WFP_E_All_Data_(Normalized).zip",  # different layout
        "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip",  # different layout
        "Environment_Temperature_change_E_All_Data_(Normalized).zip",  # monthly
    ]

    if filename in skip_files:
        return True

    return False


def download_individual_datasets(md, sources_dir):
    """Download individual FAO datasets based on metadata"""
    datasets = md["Datasets"]["Dataset"]
    print(f"Found {len(datasets)} datasets to download")

    # Create sources directory if it doesn't exist
    os.makedirs(sources_dir, exist_ok=True)

    # Track successful, failed, and skipped downloads
    successful_downloads = []
    failed_downloads = []
    skipped_downloads = []

    for i, dataset in enumerate(datasets):
        dataset_code = dataset["DatasetCode"]
        dataset_name = dataset["DatasetName"]
        file_location = dataset["FileLocation"]

        # Check if dataset should be skipped
        if should_skip_dataset(dataset_name, file_location):
            filename = file_location.split("/")[-1]
            print(f"[{i+1}/{len(datasets)}] Skipping {dataset_code}: {filename}")
            skipped_downloads.append(
                {
                    "dataset_code": dataset_code,
                    "filename": filename,
                    "status": "skipped",
                }
            )
            continue

        # Extract filename from URL
        filename = file_location.split("/")[-1]
        output_path = osp.join(sources_dir, filename)

        print(f"[{i+1}/{len(datasets)}] Downloading {dataset_code}: {filename}")

        try:
            # Skip if file already exists and is not empty
            if osp.exists(output_path) and osp.getsize(output_path) > 0:
                print(f"  File already exists, skipping: {filename}")
                successful_downloads.append(
                    {
                        "dataset_code": dataset_code,
                        "filename": filename,
                        "status": "downloaded",
                    }
                )
                continue

            download(file_location, output_path)
            successful_downloads.append(
                {
                    "dataset_code": dataset_code,
                    "filename": filename,
                    "status": "downloaded",
                }
            )
            print(f"  Successfully downloaded: {filename}")

        except Exception as e:
            print(f"  Failed to download {filename}: {str(e)}")
            failed_downloads.append(
                {"dataset_code": dataset_code, "filename": filename, "error": str(e)}
            )

    # Print summary
    print("\nDownload Summary:")
    print(f"  Successful: {len(successful_downloads)}")
    print(f"  Skipped: {len(skipped_downloads)}")
    print(f"  Failed: {len(failed_downloads)}")

    if failed_downloads:
        print("\nFailed downloads:")
        for fail in failed_downloads:
            print(f'  - {fail["dataset_code"]}: {fail["filename"]} ({fail["error"]})')

    return successful_downloads, failed_downloads


if __name__ == "__main__":
    print("Downloading metadata to " + metadata_out_file)
    if os.path.exists(metadata_out_file):
        print(f"  Metadata file already exists: {metadata_out_file}")
    else:
        download(metadata_url, metadata_out_file)

    print("Converting datasets metadata to csv...")
    md = json.load(open(metadata_out_file, encoding="latin1"))
    df = pd.DataFrame.from_records(md["Datasets"]["Dataset"])
    df.to_csv(dataset_list_file, index=False)

    print("Downloading individual datasets...")
    successful, failed = download_individual_datasets(md, sources_dir)

    print("Done.")
