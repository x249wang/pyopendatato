# -*- coding: utf-8 -*-

import json
import os
import urllib
from unittest import mock

import ckanapi
import geopandas
import pandas as pd
import pytest
import responses
from shapely.geometry import Point

from pyopendatato.ckanTO import ckanTO

DATASTORE_SEARCH_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/action/datastore_search"
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def test_get_resource_not_found():

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.side_effect = ckanapi.CKANAPIError

        c = ckanTO()

        with pytest.raises(ckanapi.CKANAPIError):
            c.get_resource(resource_id="123")


@pytest.mark.parametrize(
    "format",
    [
        "GSHEET",
        "PDF",
        "KML",
        "WEB",
        "SAV",
        "XML",
        "XSD",
        "IGE",
        "DOC",
        "DOCX",
        "",
        ".",
        "A",
    ],
)
def test_get_resource_invalid_format(format):

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = {
            "datastore_active": False,
            "format": format,
        }

        c = ckanTO()

        with pytest.raises(Exception):
            c.get_resource(resource_id="123")


@responses.activate
def test_get_resource_datastore():

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = json.load(
            open(os.path.join(FIXTURES_DIR, "resource_metadata.json"), "r")
        )

        params_init = {
            "resource_id": "b9214fd7-60d1-45f3-8463-a6bd9828f8bf",
            "limit": 1,
        }

        params_full_count = {
            "resource_id": "b9214fd7-60d1-45f3-8463-a6bd9828f8bf",
            "limit": 5733,
        }

        responses.add(
            responses.GET,
            DATASTORE_SEARCH_URL + "?" + urllib.parse.urlencode(params_init),
            status=200,
            json=json.load(open(os.path.join(FIXTURES_DIR, "datastore.json"), "r")),
        )

        responses.add(
            responses.GET,
            DATASTORE_SEARCH_URL + "?" + urllib.parse.urlencode(params_full_count),
            status=200,
            json=json.load(open(os.path.join(FIXTURES_DIR, "datastore.json"), "r")),
        )

        c = ckanTO()
        data = c.get_resource(resource_id="b9214fd7-60d1-45f3-8463-a6bd9828f8bf")

        ref = pd.read_csv(
            os.path.join(FIXTURES_DIR, "datastore.csv"),
            dtype={
                "ADDRESSNUMBERTEXT": "object",
                "BARCODE": "object",
                "POSTERBOARDSTATUS": "object",
                "WARD": "object",
            },
        ).fillna("")

        assert data.round(4).equals(ref[data.columns.values].round(4))
        assert ref.round(4).equals(data[ref.columns.values].round(4))


@responses.activate
def test_get_resource_csv():

    url = "https://www.alink.com"

    with open(os.path.join(FIXTURES_DIR, "sample_csv.csv"), "rb") as content:
        responses.add(responses.GET, url, status=200, body=content.read())

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = {
            "datastore_active": False,
            "format": "CSV",
            "url": url,
            "id": "123",
            "name": "Test data",
            "last_modified": "2019-09-28",
            "package_id": "ABC",
        }

        c = ckanTO()
        data = c.get_resource(resource_id="123")

        ref = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        assert data.equals(ref)


@pytest.mark.parametrize("format", ["XLS", "XLSX", "XLSM"])
@responses.activate
def test_get_resource_excel(format):

    url = "https://www.alink.com"

    file_path = os.path.join(
        FIXTURES_DIR, "sample_" + format.lower() + "." + format.lower()
    )
    with open(file_path, "rb") as content:
        responses.add(responses.GET, url, status=200, body=content.read())

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = {
            "datastore_active": False,
            "format": format,
            "url": url,
            "id": "123",
            "name": "Test data",
            "last_modified": "2019-09-28",
            "package_id": "ABC",
        }

        c = ckanTO()
        data = c.get_resource(resource_id="123")

        ref = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        assert data.equals(ref)


@responses.activate
def test_get_resource_geojson():

    url = "https://www.alink.com"

    with open(os.path.join(FIXTURES_DIR, "sample_geojson.geojson"), "rb") as content:
        responses.add(responses.GET, url, status=200, body=content.read())

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = {
            "datastore_active": False,
            "format": "GEOJSON",
            "url": url,
            "id": "123",
            "name": "Test data",
            "last_modified": "2019-09-28",
            "package_id": "ABC",
        }

        c = ckanTO()
        data = c.get_resource(resource_id="123")

        ref = pd.DataFrame(
            {"location": ["p1", "p2"], "geometry": [Point((1, 1)), Point((2, 2))]}
        )

        assert all(data == ref)


@responses.activate
def test_get_resource_json():

    url = "https://www.alink.com"

    responses.add(
        responses.GET, url, status=200, body=b'{"key1": "hello", "key2": "world"}'
    )

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = {
            "datastore_active": False,
            "format": "JSON",
            "url": url,
            "id": "123",
            "name": "Test data",
            "last_modified": "2019-09-28",
            "package_id": "ABC",
        }

        c = ckanTO()
        data = c.get_resource(resource_id="123")

        assert data == {"key1": "hello", "key2": "world"}


@responses.activate
def test_get_resource_txt():

    url = "https://www.alink.com"

    responses.add(responses.GET, url, status=200, body=b"hello\nworld")

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = {
            "datastore_active": False,
            "format": "TXT",
            "url": url,
            "id": "123",
            "name": "Test data",
            "last_modified": "2019-09-28",
            "package_id": "ABC",
        }

        c = ckanTO()
        data = c.get_resource(resource_id="123")

        assert data == ["hello", "world"]


@responses.activate
def test_get_resource_shp():

    url = "https://www.alink.com"

    with open(os.path.join(FIXTURES_DIR, "sample_shp.zip"), "rb") as content:
        responses.add(responses.GET, url, status=200, body=content.read())

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = {
            "datastore_active": False,
            "format": "SHP",
            "url": url,
            "id": "123",
            "name": "Test data",
            "last_modified": "2019-09-28",
            "package_id": "ABC",
        }

        c = ckanTO()
        data = c.get_resource(resource_id="123")

        ref = geopandas.read_file(
            os.path.join(FIXTURES_DIR, "sample_shp/TTC_SUBWAY_LINES_WGS84.shp")
        )

        assert data.equals(ref)


@responses.activate
def test_get_resource_zip_invalid_format():

    url = "https://www.alink.com"

    with open(os.path.join(FIXTURES_DIR, "sample_zip_invalid.zip"), "rb") as content:
        responses.add(responses.GET, url, status=200, body=content.read())

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = {
            "datastore_active": False,
            "format": "ZIP",
            "url": url,
            "id": "123",
            "name": "Test data",
            "last_modified": "2019-09-28",
            "package_id": "ABC",
        }

        c = ckanTO()

        with pytest.raises(Exception):
            c.get_resource(resource_id="123")


@responses.activate
def test_get_resource_zip():

    url = "https://www.alink.com"

    with open(os.path.join(FIXTURES_DIR, "sample_zip.zip"), "rb") as content:
        responses.add(responses.GET, url, status=200, body=content.read())

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = {
            "datastore_active": False,
            "format": "ZIP",
            "url": url,
            "id": "123",
            "name": "Test data",
            "last_modified": "2019-09-28",
            "package_id": "ABC",
        }

        c = ckanTO()
        data = c.get_resource(resource_id="123")

        ref = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        assert list(data.keys()) == ["sample_csv.csv", "sample_xlsx.xlsx"]
        assert data["sample_csv.csv"].equals(ref)
        assert data["sample_xlsx.xlsx"].equals(ref)
