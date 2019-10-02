# -*- coding: utf-8 -*-

import shutil
import tempfile
from pathlib import Path

import ckanapi
import pandas as pd
import requests

from pyopendatato.utils import download_extract_zipped_file, read_datastore, read_file

OPEN_DATA_TORONTO_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

PACKAGE_INFO_COLS = [
    "id",
    "title",
    "topics",
    "excerpt",
    "formats",
    "num_resources",
    "refresh_rate",
    "last_refreshed",
    "notes",
]

RESOURCE_INFO_COLS = [
    "id",
    "name",
    "format",
    "datastore_active",
    "last_modified",
    "package_id",
    "url",
]


class ckanTO(object):
    def __init__(self):
        self.remoteckan = ckanapi.RemoteCKAN(OPEN_DATA_TORONTO_URL)

    def list_packages(self, limit=10):

        list_results = self.remoteckan.action.current_package_list_with_resources(
            limit=limit
        )

        packages_list = []
        for pkg in list_results:
            packages_list.append({k: pkg[k] for k in PACKAGE_INFO_COLS})

        packages_table = pd.DataFrame(packages_list)  # Sorted by last_refreshed date

        return packages_table[PACKAGE_INFO_COLS]

    def search_packages(self, query, limit=10):

        search_results = self.remoteckan.action.package_search(
            fq=f'title:"{query}"', rows=limit
        )

        if search_results["count"] == 0:
            print("Cannot find any packages.")
            return

        packages_list = []
        for result in search_results["results"]:
            packages_list.append(
                {k: (result[k] if k in result else "") for k in PACKAGE_INFO_COLS}
            )

        search_results_table = pd.DataFrame(packages_list)

        return search_results_table[PACKAGE_INFO_COLS]

    def get_package_metadata(self, package_id, show_resources=True):

        try:
            package = self.remoteckan.action.package_show(id=package_id)
        except ckanapi.CKANAPIError as error:
            print(f"Encountered an error - {error}")
            raise

        package_dict = {k: package[k] for k in PACKAGE_INFO_COLS}

        if show_resources:
            resource_list = []
            for resource in package["resources"]:
                resource_list.append({k: resource[k] for k in RESOURCE_INFO_COLS})
            package_dict["resources"] = resource_list

        return package_dict

    def get_resource_metadata(self, resource_id):

        try:
            resource = self.remoteckan.action.resource_show(id=resource_id)
        except ckanapi.CKANAPIError as error:
            print(f"Encountered an error - {error}")
            raise

        return {k: resource[k] for k in RESOURCE_INFO_COLS}

    def get_resource(self, resource_id):

        try:
            resource_info = self.get_resource_metadata(resource_id=resource_id)
        except ckanapi.CKANAPIError as error:
            print(f"Encountered an error - {error}")
            raise

        if resource_info["datastore_active"]:
            return read_datastore(resource_id)

        elif resource_info["format"] in [
            "CSV",
            "XLS",
            "XLSX",
            "XLSM",
            "GEOJSON",
            "JSON",
            "TXT",
        ]:

            temp_file = Path(
                tempfile.NamedTemporaryFile(suffix=resource_info["format"]).name
            )

            with requests.get(resource_info["url"]) as response:
                with open(temp_file, "wb") as out_file:
                    out_file.write(response.content)

            data = read_file(temp_file, resource_info["format"])

            temp_file.unlink()
            return data

        elif resource_info["format"] in ["SHP"]:

            temp_dir = download_extract_zipped_file(
                url=resource_info["url"], file_ext="ZIP"
            )

            temp_file = next(temp_dir.glob("*.shp"))

            data = read_file(temp_file, resource_info["format"])

            shutil.rmtree(temp_dir)
            return data

        elif resource_info["format"] in ["GZ", "RAR", "ZIP"]:

            temp_dir = download_extract_zipped_file(
                url=resource_info["url"], file_ext=resource_info["format"]
            )

            data_list = {}
            for file in temp_dir.iterdir():
                data_list[file.name] = read_file(file, file.suffix.upper()[1:])

            shutil.rmtree(temp_dir)
            return data_list

        else:
            raise Exception(
                f"{resource_info['format']} cannot be downloaded using pyopendatato. "
                "Please visit Open Data Toronto's website."
            )
