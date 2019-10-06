# pyopendatato

`pyopendatato` is a Python package for accessing resources from City of Toronto's [Open Data Portal](https://open.toronto.ca/catalogue), which runs on the [CKAN](https://ckan.org/) software. It is written in Python 3, and supports package searches, metadata retrieval about packages and resources belonging to packages, as well as data downloading.

## Installation

`pyopendatato` can be installed using

```
pip install git+https://github.com/x249wang/pyopendatato.git
```

from the command line.

## How to Use

Data under CKAN systems are organized into packages and resources. A package can be thought of as the product (e.g. air contaminant tracking), and the resources within the package refer to the various data files (e.g. chemical tracking data in csv, json and xml formats).

Most functionalities of `pyopendatato` can be accessed with the main `ckanTO` class.

```
from pyopendatato.ckanTO import ckanTO as ckanTO
ct = ckanTO()
```

### List Available Packages

To list available packages (sorted by most recently refreshed date):

```
ct.list_packages(limit = <NUMBER_OF_RESULTS_TO_RETURN>)
```

### Search Packages

To look for packages by a search term:

```
ct.search_packages(query = <SEARCH_STRING>, limit = <NUMBER_OF_RESULTS_TO_RETURN>)
```

This will return some basic information about data packages matching the search criteria, such as the package id and refresh date.


### Get Metadata

To look for metadata on a package by package id, use:

```
ct.get_package_metadata(package_id = <PACKAGE_ID>)
```

To see the list of available resources (and their resource ids), specify `show_resources=True` when retrieving package metadata (this is enabled by default).

The metadata for a specific resource can also be collected given its resource id:

```
ct.get_resource_metadata(resource_id = <RESOURCE_ID>)
```

### Download Data

Currently, `pyoepndatato` works for downloading csv, Excel (xlsx, xlsm, xls), shp, geojson, json, txt files, as well as any compressed folders containing the aforementioned file formats. The data is most often returned as a `pandas.DataFrame`, except in case of json and txt files.

Data is available at the resource level, so resource id is needed for downloading datasets:

```
ct.get_resource(resource_id = <RESOURCE_ID>)
```

## Issues

For any feedback or bug reports, please create an issue in the [Github repository](https://github.com/x249wang/pyopendatato).
