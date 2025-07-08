# -*- coding: utf-8 -*-

import os
import os.path as osp
import zipfile
import decimal
import re
import json
import tempfile

import numpy as np
import pandas as pd
import requests as r
from pandas.api.types import CategoricalDtype

from ddf_utils.str import to_concept_id, format_float_digits

sources_dir = "../datasets"
metadata_file = "../source/datasets_E.json"
dataset_list_file = "../source/dataset_list.csv"
out_dir = "../../"

# default dtypes for read_csv, we use categories to reduce memory usage.
DEFAULT_DTYPES = {
    "Area Code": "category",
    "Country Code": "category",
    "CountryCode": "category",
    "Area": "category",
    "Item Code": "category",
    "Item": "category",
    "Element Code": "category",
    "Element": "category",
    "Year": "int",
    "Year Code": "category",
    "Unit": "category",
}

URL_GROUP = "https://faostatservices.fao.org/api/v1/en/definitions/types/areagroup"
URL_AREA = "https://faostatservices.fao.org/api/v1/en/definitions/types/area"
URL_FLAG = "https://faostatservices.fao.org/api/v1/en/definitions/types/flag"


def guess_data_filename(zf: zipfile.ZipFile):
    # Note 2019-12-02: now the zip file contains 2 file, one data csv and the other flags csv.
    # we only need the data csv.
    fns = [
        f.filename
        for f in zf.filelist
        if ("Flags" not in f.filename)
        and ("Symboles" not in f.filename)
        and ("ItemCode" not in f.filename)
        and ("AreaCode" not in f.filename)
        and ("Element" not in f.filename)
        and ("Releases" not in f.filename)
        and ("Sources" not in f.filename)
        and ("Indicators" not in f.filename)
        and ("Purposes" not in f.filename)
    ]
    assert len(fns) == 1, f"there should be only one file. but {fns} found."
    return fns[0]


def get_dataset_files():
    """Get list of individual dataset files from sources directory"""
    if not osp.exists(sources_dir):
        raise ValueError(
            f"Sources directory {sources_dir} does not exist. Please run update_source.py first."
        )

    dataset_files = []
    for filename in os.listdir(sources_dir):
        if filename.endswith(".zip"):
            filepath = osp.join(sources_dir, filename)
            if osp.isfile(filepath):
                dataset_files.append(filename)

    if not dataset_files:
        raise ValueError(
            f"No dataset files found in {sources_dir}. Please run update_source.py first."
        )

    print(f"Found {len(dataset_files)} dataset files to process")
    return dataset_files


def get_dataset_prefix_mapping():
    """Create dataset prefix mapping from metadata"""
    with open(metadata_file, "r", encoding="latin1") as f:
        md = json.load(f)

    datasets = md["Datasets"]["Dataset"]
    filename_to_code = {}

    for dataset in datasets:
        file_location = dataset["FileLocation"]
        filename = file_location.split("/")[-1]
        dataset_code = dataset["DatasetCode"]
        filename_to_code[filename] = dataset_code

    # Manual settings for files not in metadata
    manual_settings = {
        "Emissions_Agriculture_Waste_Disposal_E_All_Data_(Normalized).zip": "GMEA",
        "Environment_Transport_E_All_Data_(Normalized).zip": "GMET",
        "SDG_BulkDownloads_E_All_Data_(Normalized).zip": "GMSB",
    }

    filename_to_code.update(manual_settings)
    return filename_to_code


def ordered_flag_category():
    """There are many flags in faostat that repersent data
    quality for a datapoint. But FAO doesn't sort the flags,
    so we manually pick some important ones and make a ordered
    category, for later usage
    """
    all_flags = r.get(URL_FLAG).json()
    all_flags_names = [x["Flag"] for x in all_flags["data"]]
    # when flag is empty(nan), it's official data (best quality)
    important_flags = [np.nan, "E", "F", "Ff", "A", "S"]
    # but nan is not supported as a category value in pandas, so we change it
    important_flags[0] = "_"

    flags_order = (
        pd.Series([*important_flags, *all_flags_names])
        .drop_duplicates()
        .values.tolist()
    )
    flag_cat = CategoricalDtype(flags_order, ordered=True)

    return flag_cat


def process_dataset_file(filename, dataset_code, flag_cat, geos):
    """Process a single dataset file and create datapoints files"""
    concs = []
    filepath = osp.join(sources_dir, filename)

    print(f"Processing {filename} (dataset: {dataset_code})")

    try:
        with zipfile.ZipFile(filepath, "r") as zf:
            # Get the data CSV filename
            fn_data_csv = guess_data_filename(zf)

            # Extract CSV to temporary file
            with tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False) as temp_file:
                temp_csv_path = temp_file.name
                with zf.open(fn_data_csv) as csv_file:
                    temp_file.write(csv_file.read())

        try:
            # Read the CSV data from temporary file
            df = pd.read_csv(temp_csv_path, encoding="latin1", dtype=DEFAULT_DTYPES)  # type: ignore

            # Process the data similar to the original process_file function
            def starts_with_char(x):
                return re.match("[a-zA-Z].*", x)

            def is_good_length(x):
                return len(x) < 80

            def add_domain(x):
                return " ".join([dataset_code, x])

            if "Element" in df.columns:
                groups = df.groupby(["Item Code", "Element Code"])
            else:
                groups = df.groupby("Item Code")

            for g, df_g in groups:
                if "Area Code" in df.columns:
                    country_col = "Area Code"
                elif "Country Code" in df.columns:
                    country_col = "Country Code"
                elif "CountryCode" in df.columns:
                    country_col = "CountryCode"
                else:
                    print(f"Error: column layout not supported for {filename}")
                    continue

                df_ = df_g[[country_col, "Year", "Value", "Unit", "Flag"]].copy()
                item_name = df_g.iloc[0]["Item"]

                if isinstance(g, tuple):  # groupby item code and element code
                    element_name = df_g.iloc[0]["Element"]
                    item_code = str(g[0])
                    if starts_with_char(item_code) and is_good_length(item_code):
                        concept_fullname = " - ".join([item_code, element_name])
                    elif is_good_length(item_name):
                        concept_fullname = " - ".join([item_name, element_name])
                    else:
                        concept_fullname = " - ".join([item_code, element_name])
                    indicator = " - ".join([item_name, element_name])
                else:  # only group by item code
                    item_code = str(g)
                    if starts_with_char(item_code) and is_good_length(item_code):
                        concept_fullname = item_code
                    elif is_good_length(item_name):
                        concept_fullname = item_name
                    else:
                        concept_fullname = item_code
                    indicator = item_name

                concept_id = to_concept_id(add_domain(concept_fullname))

                df_.columns = ["geo", "year", concept_id, "unit", "flag"]

                df_ = df_.dropna(subset=[concept_id])

                # don't include geos not in geo domain
                df_ = df_[df_["geo"].isin(geos)]

                if df_.empty:  # no content
                    continue
                if len(df_["unit"].unique()) > 1:
                    print(f'unit not unique: {concept_id}, {df_["unit"].unique()}')
                    continue  # don't proceed these indicators

                unit = df_["unit"].unique()[0]
                concs.append({"name": indicator, "concept": concept_id, "unit": unit})

                df_["flag"] = df_["flag"].fillna("_")
                df_["flag"] = df_["flag"].astype(flag_cat)
                df_ = df_.sort_values(by="flag").drop_duplicates(
                    subset=["geo", "year"], keep="first"
                )

                if df_[df_.duplicated(subset=["geo", "year"])].shape[0] > 0:
                    print(f"duplicated found in {concept_id}")

                df_ = df_[["geo", "year", concept_id]]
                try:
                    df_[concept_id] = df_[concept_id].map(format_float_digits)
                except decimal.InvalidOperation:
                    print(f"{concept_id} values seems not decimals")

                df_["geo"] = df_["geo"].astype(str)

                # Create output directory
                os.makedirs(
                    osp.join(out_dir, "datapoints", dataset_code), exist_ok=True
                )

                # Save the datapoints file
                output_file = osp.join(
                    out_dir,
                    "datapoints",
                    dataset_code,
                    f"ddf--datapoints--{concept_id}--by--geo--year.csv",
                )

                (df_.sort_values(by=["geo", "year"]).to_csv(output_file, index=False))

        finally:
            # Clean up temporary file
            if 'temp_csv_path' in locals() and osp.exists(temp_csv_path):
                os.unlink(temp_csv_path)

    except Exception as e:
        print(f"Failed to process {filename}: {str(e)}")
        # Clean up temporary file in case of exception
        if 'temp_csv_path' in locals() and osp.exists(temp_csv_path):
            os.unlink(temp_csv_path)

    return concs


def process_files(geos):
    """Process all dataset files"""
    concs = []

    # Get list of dataset files
    dataset_files = get_dataset_files()

    # Get dataset code mapping
    filename_to_code = get_dataset_prefix_mapping()

    # Get flag category
    flag_cat = ordered_flag_category()

    # Process each dataset file
    processed_count = 0
    failed_count = 0

    for filename in dataset_files:
        # Get dataset code
        dataset_code = filename_to_code.get(filename)
        if not dataset_code:
            print(f"Warning: No dataset code found for {filename}, skipping")
            failed_count += 1
            continue

        try:
            concs_from_file = process_dataset_file(
                filename, dataset_code, flag_cat, geos
            )
            concs.extend(concs_from_file)
            processed_count += 1
        except Exception as e:
            print(f"Failed to process {filename}: {str(e)}")
            failed_count += 1

    print(
        f"Processing complete: {processed_count} files processed, {failed_count} files failed"
    )
    return concs


def process_area_and_groups():
    """Process area and country groups from FAO API"""
    areagroup = r.get(URL_GROUP).json()
    area = r.get(URL_AREA).json()

    areaDf = pd.DataFrame.from_records(area["data"])
    areagroupDf = pd.DataFrame.from_records(areagroup["data"])

    area_to_group = (
        areagroupDf.groupby("Country Code")["Country Group Code"]
        .agg(lambda xs: ",".join(sorted(list(set(xs.values.tolist())))))
        .reset_index()
    )

    areaDf["is--country"] = "FALSE"
    areaDf["is--country_group"] = "FALSE"
    areaDf.loc[
        areaDf["Country Code"].isin(areagroupDf["Country Code"].values.tolist()), "is--country"
    ] = "TRUE"
    areaDf.loc[
        areaDf["Country Code"].isin(areagroupDf["Country Group Code"].values.tolist()),
        "is--country_group",
    ] = "TRUE"

    areaDf.columns = [
        "geo",
        "name",
        "end_year",
        "iso2_code",
        "iso3_code",
        "m49_code",
        "start_year",
        "is--country",
        "is--country_group",
    ]
    areaDf = areaDf[
        [
            "geo",
            "name",
            "start_year",
            "end_year",
            "iso2_code",
            "iso3_code",
            "m49_code",
            "is--country",
            "is--country_group",
        ]
    ]

    areaDf = areaDf.set_index("geo")
    area_to_group = area_to_group.set_index("Country Code")
    areaDf["country_groups"] = area_to_group.reindex(areaDf.index)["Country Group Code"]

    # TODO: not sure why there are duplicates.
    areaDf = areaDf.reset_index().drop_duplicates(subset=["geo"])
    areaDf.to_csv(osp.join(out_dir, "ddf--entities--geo.csv"), index=False)

    return areaDf


def process_concepts(concs):
    """Process and save concepts"""
    cdf = pd.DataFrame.from_records(concs)
    cdf["concept_type"] = "measure"
    cdf[cdf.duplicated(subset="concept", keep=False)].sort_values(by=["concept"])
    cdf = cdf.drop_duplicates(subset="concept")

    cdf.sort_values(by="concept").to_csv(
        osp.join(out_dir, "ddf--concepts--continuous.csv"), index=False
    )

    cdf2 = pd.DataFrame(
        [
            ["name", "string", "Name", ""],
            ["geo", "entity_domain", "Geo domain", ""],
            ["country", "entity_set", "Country", "geo"],
            ["country_group", "entity_set", "Country Group", "geo"],
            ["country_groups", "string", "Country Groups", ""],
            ["year", "time", "Year", ""],
            ["iso2_code", "string", "ISO2 Code", ""],
            ["iso3_code", "string", "ISO3 Code", ""],
            ["m49_code", "string", "M49 Code", ""],
            ["start_year", "string", "Start Year", ""],
            ["end_year", "string", "End Year", ""],
            ["domain", "string", "Domain", ""],
            ["unit", "string", "Unit", ""],
        ]
    )
    cdf2.columns = ["concept", "concept_type", "name", "domain"]

    cdf2.to_csv(osp.join(out_dir, "ddf--concepts--discrete.csv"), index=False)


def main():
    print("Processing geo entities...")
    geos = process_area_and_groups()["geo"].values

    print("Creating datapoints directory...")
    os.makedirs(osp.join(out_dir, "datapoints"), exist_ok=True)

    print("Processing datapoints from individual dataset files...")
    concs = process_files(geos)

    print("Processing concepts...")
    process_concepts(concs)

    print("Done")


if __name__ == "__main__":
    main()
