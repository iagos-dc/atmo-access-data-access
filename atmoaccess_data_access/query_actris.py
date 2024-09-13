import warnings
import io
import requests
from requests.exceptions import HTTPError
import pandas as pd
import xarray as xr

REST_URL_PATH = "https://prod-actris-md.nilu.no/"
REST_URL_STATIONS = REST_URL_PATH + "facilities"
REST_URL_VARIABLES = REST_URL_PATH + "vocabulary/contentattribute"
REST_URL_DOWNLOAD = REST_URL_PATH + "metadata/"

STATIC_PARAMETERS = ["latitude", "longitude", "air_pressure", "barometric_altitude"]

MAPPING_ECV_ACTRIS = {
    'Aerosol Optical Properties': ['aerosol particle light absorption coefficient'],
    'Aerosol Chemical Properties': ['aerosol particle elemental carbon mass concentration', 'aerosol particle organic carbon mass concentration', 'aerosol particle total carbon mass concentration'],
    'Aerosol Physical Properties': ['aerosol particle number size distribution	'],
    'NO2' : ['nitrogen dioxide amount fraction', 'nitrogen dioxide mass concentration', 'nitrogen dioxide number concentration'],   
}

def _reverse_mapping(mapping):
    ret = {}
    for key, values in mapping.items():
        for value in values:
            if value not in ret:
                ret[value] = []
            ret[value].append(key)
    return ret


MAPPING_ACTRIS_ECV = _reverse_mapping(MAPPING_ECV_ACTRIS)

def get_list_platforms():
    """
    Retrieves a list of ACTRIS facilities (ACTRIS National Facility - In Progress):
    - 'short_name'
    - 'long_name'
    - 'longitude'
    - 'latitude'
    - 'altitude'
    :return: list of dict
    """
    try:
        response = requests.get(REST_URL_STATIONS)
        response.raise_for_status()

        all_facilities = []

        for facility in response.json():
                
                if facility.get('actris_national_facility') == True:
                    all_facilities.append(
                        {
                            'short_name': facility['identifier'],
                            'latitude': facility['lat'],
                            'longitude': facility['lon'],
                            'long_name': facility['name'],
                            'URI': 'https://prod-actris-md.nilu.no/facilities/{0}'.format(facility['identifier']),
                            'altitude': facility['alt']})
                else:
                    pass

        return all_facilities

    except HTTPError as http_err:
        warnings.warn(f'HTTP error occurred: {http_err}')
        raise
    except Exception as err:
        warnings.warn(f'Other error occurred: {err}')
        raise


def get_list_variables():
    """
    Retrieves a ACTRIS mapping between variable names and ECV names. The mapping is a list of dict. Each dict has
    the keys
    - 'variable_name'
    - 'ECV_name'
    and associates a single ACTRIS variable with one or many ECV name(s)
    e.g. {'variable_name': 'nitrogen dioxide amount fraction', 'ECV_name': ['NO2']}
    :return: list of dict
    """
    try:
        response = requests.get(REST_URL_VARIABLES)
        response.raise_for_status()
        jsonResponse = response.json()
        variables = []
        for item in jsonResponse:
            if item['label'] in MAPPING_ACTRIS_ECV:
                variable = {
                    'variable_name': item['label'],
                    'ECV_name': MAPPING_ACTRIS_ECV[item['label']]
                }
                variables.append(variable)
        return variables
    
    except HTTPError as http_err:
        warnings.warn(f'HTTP error occurred: {http_err}')
        raise
    except Exception as err:
        warnings.warn(f'Other error occurred: {err}')
        raise

def query_datasets_stations(codes, variables_list=None, temporal_extent=None):
    """
    Query the ACTRIS database for metadata of datasets satisfying the specified criteria.
    :param codes: a list of ACTRIS facility identifiers (short_name); selects datasets with the correct facility identifier in the list
    :param variables_list: optional; a list of ECV names; selects datasets with ecv_variables not disjoint with the list
    :param temporal_extent: optional; a list/tuple of the form (start_date, end_date); start_date and end_date
    must be parsable with pandas.to_datetime; selects datasets with time_period overlapping temporal_extent intevral
    :return: a list of dict with the keys:
    """
    all_datasets = []

    for code in codes:
        page = 0
        while True:
            try:
                url = f"{REST_URL_PATH}/metadata/facility/{code}/page/{page}"
                response = requests.get(url)
                response.raise_for_status()
                datasets = response.json()
                
                if not datasets:
                    break

                all_datasets.extend(datasets)
                page += 1
            except HTTPError as http_err:
                warnings.warn(f'HTTP error occurred while querying ACTRIS datasets for station={code}, page={page}: {http_err}')
                raise
            except Exception as err:
                warnings.warn(f'Other error occurred while querying ACTRIS datasets for station={code}, page={page}: {err}')
                raise

    if variables_list is not None:
        _variables = set(variables_list)
        all_datasets = filter(lambda ds: not _variables.isdisjoint(ds['ecv_variables']), all_datasets)
    if temporal_extent is not None:
        t0, t1 = map(pd.to_datetime, temporal_extent)
        all_datasets = filter(
            lambda ds: not (pd.to_datetime(ds['time_period'][0]) > t1 or pd.to_datetime(ds['time_period'][1]) < t0),
            all_datasets
        )

    return list(all_datasets)

def read_dataset(variables_list=None):
    """
    Retrieves a dataset identified by dataset_id and selects variables listed in variables_list.
    :param dataset_id: str; an identifier (e.g. an url) of the dataset to retrieve
    :param variables_list: list of str, optional; a list of ECV names
    :return: list of dataset URLs
    """

    all_datasets = []

    for variable in variables_list:
        page = 0
        while True:
            try:
                url = f"{REST_URL_PATH}metadata/content/{variable}/page/{page}"
                response = requests.get(url)
                response.raise_for_status()
                datasets = response.json()
                
                if not datasets:
                    break

                all_datasets.extend(datasets)
                page += 1
            except HTTPError as http_err:
                warnings.warn(f'HTTP error occurred while querying ACTRIS datasets for variable={variable}, page={page}: {http_err}')
                raise
            except Exception as err:
                warnings.warn(f'Other error occurred while querying ACTRIS datasets for variable={variable}, page={page}: {err}')
                raise

    dataset_urls = []
    for dataset in all_datasets:
        md_distribution_information = dataset.get('md_distribution_information', [])
        for info in md_distribution_information:
            dataset_url = info.get('dataset_url')
            if dataset_url:
                dataset_urls.append(dataset_url)

    return dataset_urls


if __name__ == "__main__":
    #print(query_datasets_stations(['x0z5']))
    print('Read dataset function')
    print(read_dataset(variables_list=['aerosol particle number size distribution']))
    print('End read dataset function')

