# -*- coding: utf-8 -*-

from ddf_utils.factory.common import download


url = 'http://fenixservices.fao.org/faostat/static/bulkdownloads/FAOSTAT.zip'
out_file = '../source/FAOSTAT.zip'

if __name__ == '__main__':
    print('downloading file to ' + out_file)
    download(url, out_file)
    print('Done.')
