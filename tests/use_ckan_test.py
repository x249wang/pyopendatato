# -*- coding: utf-8 -*-

import json
import os
from unittest import mock

import ckanapi
import pandas as pd
import pytest

from pyopendatato.ckanTO import ckanTO

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def test_list_packages():

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.current_package_list_with_resources.return_value = json.load(
            open(os.path.join(FIXTURES_DIR, "package_list.json"), "r")
        )

        c = ckanTO()
        r = c.list_packages(limit=10)

        ref = pd.read_csv(os.path.join(FIXTURES_DIR, "package_list_cleaned.csv"))

        assert r.equals(ref)


def test_search_package_not_found(capfd):

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.package_search.return_value = {
            "count": 0,
            "sort": "score desc, metadata_modified desc",
            "facets": {},
            "results": [],
            "search_facets": {},
        }

        c = ckanTO()
        c.search_packages("test")

        out, _ = capfd.readouterr()
        assert out == "Cannot find any packages.\n"


def test_search_package():

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.package_search.return_value = json.load(
            open(os.path.join(FIXTURES_DIR, "package_search_list.json"), "r")
        )

        c = ckanTO()
        r = c.search_packages("bus")

        ref = pd.read_csv(os.path.join(FIXTURES_DIR, "package_search_list.csv")).fillna(
            ""
        )

        assert r.equals(ref)


def test_package_metadata_not_found():

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.package_show.side_effect = ckanapi.CKANAPIError

        c = ckanTO()

        with pytest.raises(ckanapi.CKANAPIError):
            c.get_package_metadata(package_id="123")


def test_package_metadata():

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.package_show.return_value = json.load(
            open(os.path.join(FIXTURES_DIR, "package_metadata.json"), "r")
        )

        c = ckanTO()
        pkg_info = c.get_package_metadata(
            package_id="1db34737-ffad-489d-a590-9171d500d453"
        )

        ref = json.load(
            open(os.path.join(FIXTURES_DIR, "package_metadata_cleaned.json"), "r")
        )

        assert pkg_info == ref


def test_resource_metadata_not_found():

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.side_effect = ckanapi.CKANAPIError

        c = ckanTO()

        with pytest.raises(ckanapi.CKANAPIError):
            c.get_resource_metadata(resource_id="123")


def test_resource_metadata():

    with mock.patch("ckanapi.RemoteCKAN") as mockCKAN:

        mock_ckan = mockCKAN()
        mock_ckan.action.resource_show.return_value = json.load(
            open(os.path.join(FIXTURES_DIR, "resource_metadata.json"), "r")
        )

        c = ckanTO()
        resource_info = c.get_resource_metadata(
            resource_id="b9214fd7-60d1-45f3-8463-a6bd9828f8bf"
        )

        ref = json.load(
            open(os.path.join(FIXTURES_DIR, "resource_metadata_cleaned.json"), "r")
        )

        assert resource_info == ref
