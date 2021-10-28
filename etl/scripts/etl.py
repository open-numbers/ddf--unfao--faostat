#-*- coding: utf-8 -*-

import os
import os.path as osp
import zipfile
from io import BytesIO
from tempfile import mkdtemp, mktemp

import numpy as np
import pandas as pd
import requests as r
from pandas.api.types import CategoricalDtype

from ddf_utils.str import to_concept_id, format_float_digits

source_file = '../source/FAOSTAT.zip'
out_dir = '../../'

# default dtypes for read_csv, we use categories to reduce memory usage.
DEFAULT_DTYPES = {
    'Area Code': 'category',
    'Country Code': 'category',
    'CountryCode': 'category',
    'Area': 'category',
    'Item Code': 'category',
    'Item': 'category',
    'Element Code': 'category',
    'Element': 'category',
    'Year': 'int',
    'Year Code': 'category',
    'Unit': 'category'}

URL_GROUP = 'http://fenixservices.fao.org/faostat/api/v1/en/definitions/types/areagroup'
URL_AREA = 'http://fenixservices.fao.org/faostat/api/v1/en/definitions/types/area'
URL_FLAG = 'http://fenixservices.fao.org/faostat/api/v1/en/definitions/types/flag'


def guess_data_filename(zf: zipfile.ZipFile):
    # Note 2019-12-02: now the zip file contains 2 file, one data csv and the other flags csv.
    # we only need the data csv.
    fns = [f.filename for f in zf.filelist if
	   ('Flags' not in f.filename) and
           ('Symboles' not in f.filename) and
           ('ItemCode' not in f.filename)]
    assert len(fns) == 1, f"there should be only one file. but {fns} found."
    return fns[0]


def scan_skip_files(zf):
    """reads a zipfile object and then reads all zipfiles inside,
    and then prints out and returns zip files that can't be read or
    will be skipped intentionally
    """
    skips = []
    for f in zf.filelist:
        fn = f.filename
        # Food security/Survey/Monthly/Archive data have different column layout
        # so we skip them.
        if ('Security' in fn or 'Survey' in fn or 'Monthly' in fn or 'Archive' in fn):
            skips.append(fn)
            continue
        if fn in ['Producción_Cultivos_S_Todos_los_Datos.zip',  # not in English
                  'Employment_Indicators_E_All_Data_(Normalized).zip',   # different layout
                  'Food_Aid_Shipments_WFP_E_All_Data_(Normalized).zip',  # different layout
                  'Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip',  # different layout
                  'Environment_Temperature_change_E_All_Data_(Normalized).zip']:  # monthly
            skips.append(fn)
            continue
        b = BytesIO(zf.read(fn))
        zf_data_csv = zipfile.ZipFile(b)
        fn_data_csv = guess_data_filename(zf_data_csv)
        b_data_csv = BytesIO(zf_data_csv.read(fn_data_csv))
        try:
            next(pd.read_csv(b_data_csv, encoding='latin1', chunksize=1))
        except NotImplementedError:
            skips.append(fn)
    return skips


def ordered_flag_category():
    """There are many flags in faostat that repersent data
    quality for a datapoint. But FAO doesn't sort the flags,
    so we manually pick some important ones and make a ordered
    category, for later usage
    """
    all_flags = r.get(URL_FLAG).json()
    all_flags_names = [x['Flag'] for x in all_flags['data']]
    # when flag is empty(nan), it's official data (best quality)
    important_flags = [np.nan, 'E', 'F', 'Ff', 'A', 'S']
    # but nan is not supported as a category value in pandas, so we change it
    important_flags[0] = '_'

    flags_order = pd.Series([*important_flags, *all_flags_names]).drop_duplicates().values.tolist()
    flag_cat = CategoricalDtype(flags_order, ordered=True)

    return flag_cat


def get_domains(zf):
    """create a abbreviate(domain) for each file in the zipfile
    """
    datasets = []
    for f in zf.filelist:
        n = f.filename
        ns = n.split('_')[:3]
        count = 0
        d = []
        for x in ns:
            if x == '':
                continue
            d.append(x[0].upper())
            count = count + 1
            if count > 3:
                break
        d = ''.join(d)

        i = 1
        d_ = d
        while d_ in datasets:
            d_ = '{}{}'.format(d, i)
            i = i + 1
        datasets.append(d_)
    fns = [x.filename for x in zf.filelist]
    domains = dict(zip(fns, datasets))
    return domains


def process_file(zf, f, domains, flag_cat, geos):
    """process a file in zf, create datapoints files and return all concepts"""
    concs = []
    # file_contents = zf.read(f)
    tmpfile = mktemp()
    with open(tmpfile, 'wb') as tf:
        with zf.open(f) as z:
            # print(tmpfile)
            tf.write(z.read())
            tf.flush()
    # load the actual csv from the zipped file.
    zf2 = zipfile.ZipFile(tmpfile)
    fn_data_csv = guess_data_filename(zf2)
    data_csv = BytesIO(zf2.read(fn_data_csv))
    df = pd.read_csv(data_csv, encoding='latin1', dtype=DEFAULT_DTYPES)

    if 'Element' in df.columns:
        groups = df.groupby(['Item', 'Element'])
    else:
        groups = df.groupby('Item')

    for g, df_g in groups:
        if 'Area Code' in df.columns:
            df_ = df_g[['Area Code', 'Year', 'Value', 'Unit', 'Flag']].copy()
        elif 'Country Code' in df.columns:
            df_ = df_g[['Country Code', 'Year', 'Value', 'Unit', 'Flag']].copy()
        elif 'CountryCode' in df.columns:
            df_ = df_g[['CountryCode', 'Year', 'Value', 'Unit', 'Flag']].copy()
        else:
            raise KeyError(df.columns)

        if isinstance(g, str):
            indicator = g
        else:
            indicator = ' - '.join(g)
        concept_id = to_concept_id(indicator + ' ' + domains[f])

        df_.columns = ['geo', 'year', concept_id, 'unit', 'flag']

        df_ = df_.dropna(subset=[concept_id])

        # don't include geos not in geo domain
        df_ = df_[df_['geo'].isin(geos)]

        if df_.empty:  # no content
            continue
        if len(df_['unit'].unique()) > 1:
            print('unit not unique:', concept_id, df_['unit'].unique())
            continue  # don't proceed these indicators

        unit = df_['unit'].unique()[0]
        concs.append({
            'name': indicator,
            'concept': concept_id,
            'unit': unit
        })

        df_['flag'] = df_['flag'].fillna('_')
        df_['flag'] = df_['flag'].astype(flag_cat)
        df_ = df_.sort_values(by='flag').drop_duplicates(subset=['geo', 'year'], keep='first')

        if df_[df_.duplicated(subset=['geo', 'year'])].shape[0] > 0:
            print('duplicated found in {}'.format(concept_id))

        df_ = df_[['geo', 'year', concept_id]]
        df_[concept_id] = df_[concept_id].map(format_float_digits)
        df_['geo'] = df_['geo'].astype(str)
        (df_
         .sort_values(by=['geo', 'year'])
         .to_csv(osp.join(out_dir,
                          'datapoints/ddf--datapoints--{}--by--geo--year.csv'.format(concept_id)),
                 index=False))

    return concs


def process_files(zf, geos):
    concs = []
    domains = get_domains(zf)
    skip_files = scan_skip_files(zf)
    flag_cat = ordered_flag_category()
    for f in zf.filelist:
        if f.filename in skip_files:
            print('skipping file: ', f.filename)
            continue
        print(f.filename)
        try:
            concs_ = process_file(zf, f.filename, domains, flag_cat, geos)
        except (KeyError, ValueError) as e:
            print('failed', end=',')
            print(e)
            continue
        [concs.append(x) for x in concs_]
    return concs


def process_area_and_groups():

    areagroup = r.get(URL_GROUP).json()
    area = r.get(URL_AREA).json()

    areaDf = pd.DataFrame.from_records(area['data'])
    areagroupDf = pd.DataFrame.from_records(areagroup['data'])

    area_to_group = (areagroupDf.groupby('Country Code')['Country Group Code']
                     .agg(lambda xs: ','.join(sorted(list(set(xs.values.tolist())))))
                     .reset_index())

    areaDf['is--country'] = 'FALSE'
    areaDf['is--country_group'] = 'FALSE'
    areaDf.loc[areaDf['Country Code'].isin(areagroupDf['Country Code'].values), 'is--country'] = 'TRUE'
    areaDf.loc[areaDf['Country Code'].isin(areagroupDf['Country Group Code'].values), 'is--country_group'] = 'TRUE'

    areaDf.columns = ['geo', 'name', 'end_year',
                      'iso2_code', 'iso3_code', 'm49_code',
                      'start_year', 'is--country', 'is--country_group']
    areaDf = areaDf[['geo', 'name', 'start_year', 'end_year',
                     'iso2_code', 'iso3_code', 'm49_code',
                     'is--country', 'is--country_group']]

    areaDf = areaDf.set_index('geo')
    area_to_group = area_to_group.set_index('Country Code')
    areaDf['country_groups'] = area_to_group.reindex(areaDf.index)['Country Group Code']

    # TODO: not sure why there are duplicates.
    areaDf = areaDf.reset_index().drop_duplicates(subset=['geo'])
    areaDf.to_csv(osp.join(out_dir, 'ddf--entities--geo.csv'), index=False)

    return areaDf


def process_concepts(concs):
    cdf = pd.DataFrame.from_records(concs)
    cdf['concept_type'] = 'measure'
    cdf[cdf.duplicated(subset='concept', keep=False)].sort_values('concept')
    cdf = cdf.drop_duplicates(subset='concept')

    cdf.sort_values(by='concept').to_csv(osp.join(out_dir, 'ddf--concepts--continuous.csv'), index=False)

    cdf2 = pd.DataFrame([
        ['name', 'string', 'Name', ''],
        ['geo', 'entity_domain', 'Geo domain', ''],
        ['country', 'entity_set', 'Country', 'geo'],
        ['country_group', 'entity_set', 'Country Group', 'geo'],
        ['country_groups', 'string', 'Country Groups', ''],
        ['year', 'time', 'Year', ''],
        ['iso2_code', 'string', 'ISO2 Code', ''],
        ['iso3_code', 'string', 'ISO3 Code', ''],
        ['m49_code', 'string', 'M49 Code', ''],
        ['start_year', 'string', 'Start Year', ''],
        ['end_year', 'string', 'End Year', ''],
        ['domain', 'string', 'Domain', ''],
        ['unit', 'string', 'Unit', '']])
    cdf2.columns = ['concept', 'concept_type', 'name', 'domain']

    cdf2.to_csv(osp.join(out_dir, 'ddf--concepts--discrete.csv'), index=False)


def main():
    print('processing geo entities...')
    geos = process_area_and_groups()['geo'].values
    os.makedirs(osp.join(out_dir, 'datapoints'), exist_ok=True)
    zf = zipfile.ZipFile(source_file)
    print('processing datapoints...')
    concs = process_files(zf, geos)
    print('processing concepts...')
    process_concepts(concs)
    print('Done')


if __name__ == '__main__':
    main()
