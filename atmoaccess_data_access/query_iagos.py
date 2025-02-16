import warnings
import io
import requests
from requests.exceptions import HTTPError
import pandas as pd
import xarray as xr

REST_URL_PATH = "https://services.iagos-data.fr/prod/v2.0/"
REST_URL_STATIONS = REST_URL_PATH + "airports/l3"
REST_URL_REGIONS = REST_URL_PATH + "regions"
REST_URL_VARIABLES = REST_URL_PATH + "parameters/public"
REST_URL_SEARCH = REST_URL_PATH + "l3/search?codes="
REST_URL_DOWNLOAD = REST_URL_PATH + "l3/loadNetcdfFile"

STATIC_PARAMETERS = ["latitude", "longitude", "air_pressure", "barometric_altitude"]

MAPPING_ECV_IAGOS = {
    "Temperature (near surface)": ["air_temperature"],
    "Water Vapour (surface)": ["mole_fraction_of_water_vapor_in_air", "relative_humidity"],
    "Temperature (upper-air)": ["air_temperature"],
    "Water Vapour (upper air)": ["mole_fraction_of_water_vapor_in_air", "relative_humidity"],
    "Cloud Properties": ["number_concentration_of_cloud_liquid_water_particles_in_air"],
    "Wind speed and direction (upper-air)": ["wind_speed", "wind_from_direction"],
    "Carbon Dioxide": ["mole_fraction_of_carbon_dioxide_in_air"],
    "Methane": ["mole_fraction_of_methane_in_air"],
    "Ozone": ["mole_fraction_of_ozone_in_air"],
    "Carbon Monoxide": ["mole_fraction_of_carbon_monoxide_in_air"],
    "NO2": ["mole_fraction_of_nitrogen_dioxide_in_air"]
}

# this is not used
MAPPING_CF_IAGOS = {
    "air_temperature": "air_temp",
    "mole_fraction_of_water_vapor_in_air": "H2O_gas",
    "relative_humidity": "RHL",
    "mole_fraction_of_methane_in_air": "CH4",
    "number_concentration_of_cloud_liquid_water_particles_in_air": "cloud",
    "mole_fraction_of_carbon_monoxide_in_air": "CO",
    "mole_fraction_of_carbon_dioxide_in_air": "CO2",
    "mole_fraction_of_nitrogen_dioxide_in_air": "NO2",
    "mole_fraction_of_ozone_in_air": "O3"
}


def _reverse_mapping(mapping):
    ret = {}
    for key, values in mapping.items():
        for value in values:
            if value not in ret:
                ret[value] = []
            ret[value].append(key)
    return ret


MAPPING_IAGOS_ECV = _reverse_mapping(MAPPING_ECV_IAGOS)


def get_list_platforms():
    """
    Retrieves a list of IAGOS platforms (airports). Each platform is described with a dictionary containing the keys:
    - 'short_name'
    - 'long_name'
    - 'longitude'
    - 'latitude'
    - 'altitude'
    e.g. {'short_name': 'FRA', 'long_name': 'Frankfurt', 'longitude': 8.54312515258789, 'latitude': 50.02642059326172, 'altitude': 111}.
    :return: list of dict
    """
    try:
        response = requests.get(REST_URL_STATIONS)
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        warnings.warn(f'HTTP error occurred: {http_err}')
        raise
    except Exception as err:
        warnings.warn(f'Other error occurred: {err}')
        raise


def get_list_regions():
    """
    Note: this function is specific to IAGOS and is not a part of common data access API.
    Retrieves a list of IAGOS regions. Each region is described with a dictionary containing the keys:
    - 'short_name'
    - 'long_name'
    - 'longitude_min'
    - 'longitude_max'
    - 'latitude_min'
    - 'latitude_max'
    - 'url'
    :return: list of dict
    """
    try:
        response = requests.get(REST_URL_REGIONS)
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        warnings.warn(f'HTTP error occurred: {http_err}')
        raise
    except Exception as err:
        warnings.warn(f'Other error occurred: {err}')
        raise


def get_list_variables():
    """
    Retrieves a IAGOS mapping between variable names and ECV names. The mapping is a list of dict. Each dict has
    the keys
    - 'variable_name'
    - 'ECV_name'
    and associates a single IAGOS variable with one or many ECV name(s)
    e.g. {'variable_name': 'mole_fraction_of_carbon_monoxide_in_air', 'ECV_name': ['Carbon Monoxide']}
    :return: list of dict
    """
    try:
        response = requests.get(REST_URL_VARIABLES)
        response.raise_for_status()
        jsonResponse = response.json()
        ret = []
        done = []
        for item in jsonResponse:
            if item['cf_standard_name'] in MAPPING_IAGOS_ECV and item['cf_standard_name'] not in done:
                variable = {
                    'variable_name': item['cf_standard_name'],
                    'ECV_name': MAPPING_IAGOS_ECV[item['cf_standard_name']]
                }
                done.append(item['cf_standard_name'])
                ret.append(variable)
        return ret
    except HTTPError as http_err:
        warnings.warn(f'HTTP error occurred: {http_err}')
        raise
    except Exception as err:
        warnings.warn(f'Other error occurred: {err}')
        raise


def query_datasets(codes, variables_list=None, temporal_extent=None):
    """
    Query the IAGOS database for metadata of datasets satisfying the specified criteria.
    :param codes: a list of IAGOS platforms codes (short_name); selects datasets with platform_id in the list
    :param variables_list: optional; a list of ECV names; selects datasets with ecv_variables not disjoint with the list
    :param temporal_extent: optional; a list/tuple of the form (start_date, end_date); start_date and end_date
    must be parsable with pandas.to_datetime; selects datasets with time_period overlapping temporal_extent intevral
    :return: a list of dict with the keys:
    - 'title'
    - 'urls'
    - 'ecv_variables'
    - 'time_period'
    - 'platform_id'
    """
    _codes = ','.join(codes)

    try:
        url = REST_URL_SEARCH + _codes
        response = requests.get(url)
        response.raise_for_status()
        datasets = response.json()
    except HTTPError as http_err:
        warnings.warn(f'HTTP error occurred while querying IAGOS datasets for stations={codes}: {http_err}')
        raise
    except Exception as err:
        warnings.warn(f'Other error occurred while querying IAGOS datasets for stations={codes}: {err}')
        raise

    if variables_list is not None:
        _variables = set(variables_list)
        datasets = filter(lambda ds: not _variables.isdisjoint(ds['ecv_variables']), datasets)
    if temporal_extent is not None:
        t0, t1 = map(lambda t: pd.to_datetime(t, utc=True), temporal_extent)
        datasets = filter(
            lambda ds: not (pd.to_datetime(ds['time_period'][0], utc=True) > t1 or pd.to_datetime(ds['time_period'][1], utc=True) < t0),
            datasets
        )

    return list(datasets)


def read_dataset(dataset_id, variables_list=None):
    """
    Retrieves a dataset identified by dataset_id and selects variables listed in variables_list.
    :param dataset_id: list of dict with keys 'url', 'type', e.g.
    [{'url': 'https://doi.org/10.25326/638#vp_daily_FRA.nc', 'type': 'LANDING_PAGE'}]
    :param variables_list: list of str, optional; a list of ECV names
    :return: xarray.Dataset object with at least one variable, otherwise returns None
    """
    if variables_list is None:
        variables_list = list(MAPPING_ECV_IAGOS)
    variables_set = set(variables_list)
    try:
        # retrieve an url of LANDING_PAGE type from dataset_id
        url = None
        for _url_info in dataset_id:
            if _url_info['type'].upper() == 'LANDING_PAGE':
                url = _url_info['url']
                break
        if url is None:
            # when no url of LANDING_PAGE type found, take a first one and hope for the best...
            url = dataset_id[0]['url']

        request_url = REST_URL_DOWNLOAD + "?fileId=" + url.replace("#", "%23")
        response = requests.get(request_url)
        response.raise_for_status()
        with io.BytesIO(response.content) as buf:
            with xr.open_dataset(buf, engine='h5netcdf') as ds:
                varlist = []
                for varname, da in ds.data_vars.items():
                    if 'standard_name' not in da.attrs:
                        continue
                    std_name = da.attrs['standard_name']
                    ecv_names = MAPPING_IAGOS_ECV.get(std_name, [])
                    if std_name not in STATIC_PARAMETERS and variables_set.isdisjoint(ecv_names):
                        continue
                    varlist.append(varname)
                return ds[varlist].load() if varlist else None
    except Exception as e:
        raise RuntimeError(f'Reading the IAGOS dataset failed: {dataset_id}') from e


if __name__ == "__main__":
    print(get_list_platforms())
    print(get_list_regions())
    print(get_list_variables())
    print(query_datasets_stations(['FRA', 'NAt']))
    for dataset in query_datasets_stations(['FRA', 'NAt']):
        ds = read_dataset(dataset['urls'], ['Carbon Monoxide', 'Ozone'])
        print(ds)
