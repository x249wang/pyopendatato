
import pandas as pd
from ckanapi import RemoteCKAN
import requests
import json

opentoronto = RemoteCKAN('https://ckan0.cf.opendata.inter.prod-toronto.ca')

n = len(opentoronto.action.package_list())
a = opentoronto.action.current_package_list_with_resources(limit = n)

# opentoronto.action.package_show(id = "1db34737-ffad-489d-a590-9171d500d453")

formats = set()
for k in a:
    if 'formats' in k:
        formats = formats.union(set(k['formats'].split(',')))

# {'CSV', 'DOC', 'DOCX', 'GEOJSON', 'GSHEET', 'GZ',
#  'JSON', 'KML', 'PDF', 'RAR', 'SAV', 'SHP', 'TXT', 'WEB',
#  'XLS', 'XLSM', 'XLSX', 'XML', 'XSD', 'ZIP'}

a_df = pd.DataFrame.from_dict(a)

b_df = pd.DataFrame()
for index, row in a_df.iterrows():
     r_i = opentoronto.action.package_show(id = row['id'])
     n_resources = len(r_i['resources'])
     b_df = b_df.append(pd.DataFrame(r_i['resources'], index = [0]*n_resources))   


opentoronto.action.resource_show(id = '059cde7d-21bc-4f24-a533-6c2c3fc33ef1')

# package id --> resource id --> datastore search
# if inactive, need to find another way to download
# (e.g. '8252e1ac-2e3b-4714-ab05-b2727f87c33a')


# Active datastore
tst = requests.get('https://ckan0.cf.opendata.inter.prod-toronto.ca/api/action/datastore_search',
                   params = {'id': "ea528161-00ba-473b-b69d-fd69e9ca6a7a"})
tst.encoding = 'utf-8'
tj = json.loads(tst.content)


# Inactive datastore
r = opentoronto.action.package_show(id = '1db34737-ffad-489d-a590-9171d500d453')

# xls files
a = requests.get(r['resources'][1]['url'])

with open('test_file.xls', 'wb') as f:
        f.write(a.content)

# shp files
a = requests.get('https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/c01c6d71-de1f-493d-91ba-364ce64884ac/resource/7c3b662a-1b42-4247-ba80-f1fd537e1c4a/download/ttc-subway-shapefile-wgs84.zip')

with open('test_shp.zip', 'wb') as f:
        f.write(a.content)

import shapefile
import zipfile

with zipfile.ZipFile('test_shp.zip', 'r') as zip_ref:
    zip_ref.extractall('test_shp')

import geopandas
shape = geopandas.read_file("test_shp/TTC_SUBWAY_LINES_WGS84.shp")


# geojson
import geopandas
dat = geopandas.read_file('Transit shelter data.geojson')


# gz
import tarfile
with tarfile.open("detailed-bluetooth-travel-time-2017.gz", mode="r:gz") as f:
        f.extractall()


# rar
from patoolib import extract_archive
extract_archive('landvover.rar', outdir='landvover')

# txt
# Need a warning to say it may need to actually be read in as another fmt
with open('pickup-schedule-2019.txt', 'r') as f:
        dat_txt = [l.strip() for l in f.readlines()]


# xls/xlsx/xlsm
dfs = pd.read_excel('telephone-poll-local-area-banner-1.xlsx', sheet_name = None)

dfs = pd.read_excel('pfr-program-activity-summary-data.xls', sheet_name = None)

dfs = pd.read_excel('live-green-toronto-membership-card-business-members.xlsm', 
                    sheet_name = None)


# json
with open('green-p-parking-2019.json', 'r') as f:
        greenp = json.load(f)

# Need to subset if the first level is just the name of the dataset
pd.DataFrame.from_records(greenp['carparks'])



# What the package should do:
# Search package
# List all packages
# List metadata for a package
# List resources for a package
# List metadata for a resource
# Get resource data


# class name: ckanTO
# init: RemoteCKAN
# methods:
# - search_packages
# - list_packages
# - get_package_info
# - get_resource_info
# - get_resource
