# -*- coding: utf-8 -*-

import json
import pandas as pd
from ddf_utils.factory.common import download


metadata_url = "https://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json"
url = 'http://fenixservices.fao.org/faostat/static/bulkdownloads/FAOSTAT.zip'
source_out_file = '../source/FAOSTAT.zip'
metadata_out_file = '../source/datasets.json'
dataset_list_file = '../source/dataset_list.csv'

if __name__ == '__main__':
    print('downloading source file to ' + source_out_file)
    download(url, source_out_file)
    print('downloading metadata to ' + metadata_out_file)
    download(metadata_url, metadata_out_file)
    print('convert datasets metadata to csv...')
    md = json.load(open(metadata_out_file, encoding='latin1'))
    df = pd.DataFrame.from_records(md['Datasets']['Dataset'])
    df.to_csv(dataset_list_file, index=False)
    print('Done.')
