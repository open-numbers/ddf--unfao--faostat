# -*- coding: utf-8 -*-

import requests as r


url = 'http://fenixservices.fao.org/faostat/static/bulkdownloads/FAOSTAT.zip'
out_file = '../source/FAOSTAT.zip'

def download():
    res = r.get(url, stream=True)
    with open(out_file, 'wb') as f:
        for chunk in res.iter_content(chunk_size=10240):
            if chunk:
                f.write(chunk)
        f.close()
    return


if __name__ == '__main__':
    print('downloading file to ' + out_file)
    download()
    print('Done.')
