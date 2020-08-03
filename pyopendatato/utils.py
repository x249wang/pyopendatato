# -*- coding: utf-8 -*-

import io
import json
import shutil
import tempfile
from pathlib import Path
import requests

import pandas as pd
import geopandas
import patoolib

DATASTORE_SEARCH_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/action/datastore_search"
)


def download_extract_zipped_file(url, file_ext):
    """
    Download a zipped folder to a temporary location, extract it to a temporary directory

    Parameters
    ----------
    url: str
        Url for where to download the file from
    file_ext:
        File extension

    Returns
    ----------
    pathlib.Path:
        Path to where the extracted files are saved
    """

    file_ext = file_ext.lower() if file_ext[0] == "." else "." + file_ext.lower()

    temp_file = Path(tempfile.NamedTemporaryFile(suffix=file_ext).name)
    temp_dir = Path(tempfile.TemporaryDirectory().name)

    with requests.get(url) as response:
        with open(temp_file, "wb") as out_file:
            shutil.copyfileobj(io.BytesIO(response.content), out_file)

    patoolib.extract_archive(str(temp_file), outdir=temp_dir, verbosity=0)

    temp_file.unlink()
    return temp_dir


def read_datastore(resource_id):
    """
    Retrieves data when the resource is part of the CKAN DataStore.

    Parameters
    ----------
    resource_id: str
        Id for resource

    Returns
    ----------
    pd.DataFrame:
        Data records in table format
    """

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
    """
    Retrieves data when the resource is not part of the CKAN DataStore.

    Parameters
    ----------
    filepath: pathlib.Path
        Path to where the data file is temporarily downloaded
    file_ext: str
        File extension

    Returns
    ----------
    A pandas.DataFrame, list or dict,
    or a dict where the keys are the filenames, and values are pd.DataFrame, list or dict types,
    depending on the resource file format
    """

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
    """
    Retrieves csv format data.

    Parameters
    ----------
    filepath: pathlib.Path
        Path to where the data file is temporarily downloaded

    Returns
    ----------
    pandas.DataFrame:
        Data in table format
    """

    return pd.read_csv(filepath)


def read_file_excel(filepath):
    """
    Retrieves Excel (xls, xlsx, xlsm) format data.

    Parameters
    ----------
    filepath: pathlib.Path
        Path to where the data file is temporarily downloaded

    Returns
    ----------
    pandas.DataFrame:
        Data in table format
    """

    dfs = pd.read_excel(filepath, sheet_name=None)

    if len(dfs) == 1:
        dfs = dfs[next(iter(dfs))]

    return dfs


def read_file_geojson(filepath):
    """
    Retrieves geojson format data.

    Parameters
    ----------
    filepath: pathlib.Path
        Path to where the data file is temporarily downloaded

    Returns
    ----------
    pandas.DataFrame:
        Data in table format
    """
    return geopandas.read_file(filepath)


def read_file_json(filepath):
    """
    Retrieves JSON format data.

    Parameters
    ----------
    filepath: pathlib.Path
        Path to where the data file is temporarily downloaded

    Returns
    ----------
    dict:
        Data in dict format
    """

    with open(filepath, "r") as in_file:
        data_json = json.load(in_file)

    return data_json


def read_file_txt(filepath):
    """
    Retrieves text format data.

    Parameters
    ----------
    filepath: pathlib.Path
        Path to where the data file is temporarily downloaded

    Returns
    ----------
    list:
        Data in list format, where each list element representing a line of text
    """

    with open(filepath, "r") as in_file:
        data_txt = [l.strip() for l in in_file.readlines()]
    return data_txt


def read_file_shp(filepath):
    """
    Retrieves SHP format data.

    Parameters
    ----------
    filepath: pathlib.Path
        Path to where the data file is temporarily downloaded

    Returns
    ----------
    pandas.DataFrame:
        Data in table format
    """

    return geopandas.read_file(filepath)
