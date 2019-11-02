# -*- coding: utf-8 -*-

import shutil
import tempfile
from pathlib import Path
import requests

import ckanapi
import pandas as pd

from .utils import download_extract_zipped_file, read_datastore, read_file

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
    """
    The ckanTO class is the interface for retrieving information from Toronto's Open Data Portal, which runs on CKAN.
    """

    def __init__(self):
        self.remoteckan = ckanapi.RemoteCKAN(OPEN_DATA_TORONTO_URL)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.remoteckan.close()

    def list_packages(self, limit=10):
        """
        This lists current packages in the portal.

        Parameters
        ----------
        limit: int, optional (default=10)
            Number of packages to return

        Returns
        ----------
        pandas.DataFrame:
            Table of packages along with information about them,
            such as refresh date and number of resources

        Examples
        ----------
        >>> from pyopendatato import ckanTO as ckanTO
        >>> ct = ckanTO()
        >>> ct.list_packages()
        """

        list_results = self.remoteckan.action.current_package_list_with_resources(
            limit=limit
        )

        packages_list = []
        for pkg in list_results:
            packages_list.append(
                {k: (pkg[k] if k in pkg else "") for k in PACKAGE_INFO_COLS}
            )

        packages_table = pd.DataFrame(packages_list)  # Sorted by last_refreshed date

        return packages_table[PACKAGE_INFO_COLS]

    def search_packages(self, query, limit=10):
        """
        This searches for packages by search terms.
        Returns None if no packages are found.

        Parameters
        ----------
        query: str
            Term for package search

        limit: int, optional (default=10)
            Number of results to return

        Returns
        ----------
        pandas.DataFrame:
            Table of packages matching search term, along with information about them,
            such as refresh date and number of resources

        Examples
        ----------
        >>> from pyopendatato import ckanTO as ckanTO
        >>> ct = ckanTO()
        >>> ct.search_packages(query = 'TTC', limit = 5)
        """

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
        """
        This retrieves metadata about packages.

        Parameters
        ----------
        package_id: str
            Id for package

        show_resources: boolean, option (default=True)
            Option for whether to return metadata about resources belonging to the package.
            Set this to True to obtain resource ids, which are needed for retrieving data

        Returns
        ----------
        dict:
            Dictionary with metadata about package specified,
            such as refresh date and number of resources

        Raises
        ----------
        CKANAPIError:
            When attempt to retrieve package information returns a CKANAPIError error,
            likely because the package was not found

        Examples
        ----------
        >>> from pyopendatato import ckanTO as ckanTO
        >>> ct = ckanTO()
        >>> ct.get_package_metadata("e28bc818-43d5-43f7-b5d9-bdfb4eda5feb")
        """

        try:
            package = self.remoteckan.action.package_show(id=package_id)
        except ckanapi.CKANAPIError as error:
            print(f"Encountered an error - {error}")
            raise

        package_dict = {
            k: (package[k] if k in package else "") for k in PACKAGE_INFO_COLS
        }

        if show_resources:
            resource_list = []
            for resource in package["resources"]:
                resource_list.append(
                    {
                        k: (resource[k] if k in resource else "")
                        for k in RESOURCE_INFO_COLS
                    }
                )
            package_dict["resources"] = resource_list

        return package_dict

    def list_package_resources(self, package_id):
        """
        This retrieves information on available resources from a package.

        Parameters
        ----------
        package_id: str
            Id for package

        Returns
        ----------
        pandas.DataFrame:
            DataFrame with metadata about resource specified,
            such as format and URL

        Raises
        ----------
        CKANAPIError:
            When attempt to retrieve package information returns a CKANAPIError error,
            likely because the package was not found

        Examples
        ----------
        >>> from pyopendatato import ckanTO as ckanTO
        >>> ct = ckanTO()
        >>> ct.list_package_resources("e28bc818-43d5-43f7-b5d9-bdfb4eda5feb")
        """

        try:
            package = self.remoteckan.action.package_show(id=package_id)
        except ckanapi.CKANAPIError as error:
            print(f"Encountered an error - {error}")
            raise

        resource_list = []
        for resource in package["resources"]:
            resource_list.append(
                {k: (resource[k] if k in resource else "") for k in RESOURCE_INFO_COLS}
            )

        return pd.DataFrame(resource_list)

    def get_resource_metadata(self, resource_id):
        """
        This retrieves metadata about resources.

        Parameters
        ----------
        resource_id: str
            Id for resource

        Returns
        ----------
        dict:
            Dictionary with metadata about resource specified,
            such as format and URL

        Raises
        ----------
        CKANAPIError:
            When attempt to retrieve resource information returns a CKANAPIError error,
            likely because the resource was not found

        Examples
        ----------
        >>> from pyopendatato import ckanTO as ckanTO
        >>> ct = ckanTO()
        >>> ct.get_resource_metadata("4d985c1d-9c7e-4f74-9864-73214f45eb4a")
        """

        try:
            resource = self.remoteckan.action.resource_show(id=resource_id)
        except ckanapi.CKANAPIError as error:
            print(f"Encountered an error - {error}")
            raise

        return {k: (resource[k] if k in resource else "") for k in RESOURCE_INFO_COLS}

    def get_resource(self, resource_id):
        """
        This downloads data from a given resource.

        Parameters
        ----------
        resource_id: str
            Id for resoruce

        Returns
        ----------
        A pandas.DataFrame, list or dict,
        or a dict where the values can be a pd.DataFrame, list or dict,
        depending on the resource file format

        Raises
        ----------
        CKANAPIError:
            When attempt to retrieve resource information returns a CKANAPIError error,
            likely because the resource was not found
        Exception:
            When the resource file format is not one of the following accepted values
            (csv, xls, xlsx, xlsm, geojson, json, txt, shp)

        Examples
        ----------
        >>> from pyopendatato import ckanTO as ckanTO
        >>> ct = ckanTO()
        >>> ct.get_resource("4d985c1d-9c7e-4f74-9864-73214f45eb4a")
        >>> ct.get_resource("f1bf1cef-7d09-407c-80c2-bb2a8b75abfa")
        """

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

                if file.suffix[1:] not in [
                    "csv",
                    "xls",
                    "xlsx",
                    "xlsm",
                    "geojson",
                    "json",
                    "txt",
                    "shp",
                ]:
                    raise Exception(
                        f"{file.suffix[1:]} cannot be downloaded using pyopendatato. "
                        "Please visit Open Data Toronto's website."
                    )

                data_list[file.name] = read_file(file, file.suffix.upper()[1:])

            shutil.rmtree(temp_dir)
            return data_list

        else:
            raise Exception(
                f"{resource_info['format']} cannot be downloaded using pyopendatato. "
                "Please visit Open Data Toronto's website."
            )
