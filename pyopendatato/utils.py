# -*- coding: utf-8 -*-

import io
import json
import shutil
import tempfile
from pathlib import Path

import geopandas
import pandas as pd
import patoolib
import requests

DATASTORE_SEARCH_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/action/datastore_search"
)


def download_extract_zipped_file(url, file_ext):

    file_ext = file_ext.lower()

    temp_file = Path(tempfile.NamedTemporaryFile(suffix="." + file_ext).name)
    temp_dir = Path(tempfile.TemporaryDirectory().name)

    with requests.get(url) as response:
        with open(temp_file, "wb") as out_file:
            shutil.copyfileobj(io.BytesIO(response.content), out_file)

    patoolib.extract_archive(str(temp_file), outdir=temp_dir, verbosity=0)

    temp_file.unlink()
    return temp_dir


def read_datastore(resource_id):

    r = requests.get(
        DATASTORE_SEARCH_URL, params={"resource_id": resource_id, "limit": 1}
    )

    n_records = json.loads(r.content)["result"]["total"]

    r = requests.get(
        DATASTORE_SEARCH_URL, params={"resource_id": resource_id, "limit": n_records}
    )
    r.encoding = "utf-8"

    data_json = json.loads(r.content)["result"]["records"]
    data_df = pd.DataFrame.from_records(data_json).fillna("")

    return data_df


def read_file(filepath, file_ext):

    file_ext = file_ext.lower()

    funcs = {
        "csv": read_file_csv,
        "xls": read_file_excel,
        "xlsx": read_file_excel,
        "xlsm": read_file_excel,
        "geojson": read_file_geojson,
        "json": read_file_json,
        "txt": read_file_txt,
        "shp": read_file_shp,
    }

    return funcs[file_ext](filepath)


def read_file_csv(filepath):
    return pd.read_csv(filepath)


def read_file_excel(filepath):

    dfs = pd.read_excel(filepath, sheet_name=None)

    if len(dfs) == 1:
        dfs = dfs[next(iter(dfs))]

    return dfs


def read_file_geojson(filepath):
    return geopandas.read_file(filepath)


def read_file_json(filepath):

    with open(filepath, "r") as in_file:
        data_json = json.load(in_file)

    if len(data_json) == 1 and len(data_json[list(data_json.keys())[0]]) > 1:
        data_json = data_json[list(data_json.keys())[1]]

    return data_json


def read_file_txt(filepath):
    with open(filepath, "r") as in_file:
        data_txt = [l.strip() for l in in_file.readlines()]
    return data_txt


def read_file_shp(filepath):
    return geopandas.read_file(filepath)
