import warnings
import io
import requests
from requests.exceptions import HTTPError
import pandas as pd
import xarray as xr

REST_URL_PATH = "https://prod-actris-md2.nilu.no/"
REST_URL_STATIONS = REST_URL_PATH + "facilities"
REST_URL_VARIABLES = REST_URL_PATH + "vocabulary/contentattribute"
REST_URL_DOWNLOAD = REST_URL_PATH + "metadata/"

STATIC_PARAMETERS = ["latitude", "longitude", "air_pressure", "barometric_altitude", "pressure", "relative_humidity", "temperature"]

MAPPING_ECV_ACTRIS = {'Aerosol Optical Properties': ['aerosol particle light absorption coefficient', 'aerosol particle light hemispheric backscatter coefficient', 'aerosol particle light scattering coefficient', 'aerosol particle optical depth'], 'Aerosol Chemical Properties': ['aerosol particle elemental carbon mass concentration', 'aerosol particle organic carbon mass concentration'], 'Aerosol Physical Properties': ['aerosol particle number concentration', 'cloud condensation nuclei number concentration', 'aerosol particle number size distribution', 'cloud condensation nuclei number size distribution'], 'NO2': ['nitrogen dioxide amount fraction', 'nitrogen dioxide mass concentration']}

MAPPING_ACTRIS_EBAS = {
    'aerosol_light_backscattering_coefficient': 'aerosol particle light hemispheric backscatter coefficient',
    'aerosol_light_scattering_coefficient': 'aerosol particle light scattering coefficient',
    'aerosol_absorption_coefficient':'aerosol particle light absorption coefficient',
    'aerosol_optical_depth':'aerosol particle optical depth',
    'elemental_carbon':'aerosol particle elemental carbon mass concentration', 
    'organic_carbon':'aerosol particle organic carbon mass concentration',
    'particle_number_concentration':'aerosol particle number concentration',
    'cloud_condensation_nuclei_number_concentration':'cloud condensation nuclei number concentration',
    'particle_number_size_distribution':'aerosol particle number size distribution',
    'cloud_condensation_nuclei_number_size_distribution':'cloud condensation nuclei number size distribution',
    'nitrogen_dioxide':'nitrogen dioxide amount fraction', 
    'nitrogen_dioxide':'nitrogen dioxide mass concentration', 
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
                            'URI': '{0}/facilities/{1}'.format(REST_URL_PATH, facility['identifier']),
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
    """
    if variables_list is None:
        variables_list = list(MAPPING_ECV_ACTRIS)
    else:
        variables_list = [var for var in variables_list if var in MAPPING_ECV_ACTRIS]

    variables_to_query = [var for variable in variables_list for var in MAPPING_ECV_ACTRIS[variable]]
    all_datasets = []

    for code in codes:
        for variable in variables_to_query:
            page = 0
            while True:
                try:
                    url = f"{REST_URL_PATH}/metadata/facility/{code}/content/{variable}/page/{page}"
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

    all_ecv_dataset = []

    for dataset in all_datasets:
        time_period = [dataset['ex_temporal_extent']['time_period_begin'], dataset['ex_temporal_extent']['time_period_end']]

        if temporal_extent is not None:
            t0, t1 = map(pd.to_datetime, temporal_extent)
            if pd.to_datetime(time_period[0]) > t1 or pd.to_datetime(time_period[1]) < t0:
                continue

        title = dataset['md_identification']['title']
        md_distribution_information = dataset['md_distribution_information']

        dataset_urls = [
            {'url': entry['dataset_url'], 'type': entry['protocol']}
            for entry in md_distribution_information
            if entry['protocol'] in ['OPeNDAP', 'HTTP']
        ]

        attribute_descriptions = dataset['md_content_information']['attribute_descriptions']

        mapped_ecvs = []
        for attribute in attribute_descriptions:
            try:
                ecv_names = MAPPING_ACTRIS_ECV[attribute]
                mapped_ecvs.extend(ecv_names)
            except KeyError:
                pass

        # Make the list unique
        mapped_ecvs = list(set(mapped_ecvs))

        platform_id = dataset['md_data_identification']['facility']['identifier']

        if mapped_ecvs:
            ecv_dataset = {'title': title, 'urls': dataset_urls, 'layers': None, 'ecv_variables': mapped_ecvs, 'time_period': time_period, 'platform_id': platform_id}
            all_ecv_dataset.append(ecv_dataset)

    return list(all_ecv_dataset)

def read_dataset(dataset_id, variables_list=None):
    """
    Retrieves a dataset identified by dataset_id and selects variables listed in variables_list.
    :param dataset_id: str; an identifier (e.g. an url) of the dataset to retrieve
    :param variables_list: list of str, optional; a list of ECV names
    :return: xarray.Dataset object
    """
    variables_set = set(variables_list) if variables_list is not None else None

    try:
        with xr.open_dataset(dataset_id) as ds:
            varlist = []
            for varname, da in ds.data_vars.items():
                if 'ebas_component' not in da.attrs:
                    continue
                if variables_set is not None:         

                    ebas_name = da.attrs['ebas_component']

                    if ebas_name in STATIC_PARAMETERS:
                        continue

                    actris_name = MAPPING_ACTRIS_EBAS.get(ebas_name)
                    
                    ecv_names = MAPPING_ACTRIS_ECV.get(actris_name, [])

                    if ebas_name not in STATIC_PARAMETERS and variables_set.isdisjoint(ecv_names):
                        continue

                
                
                varlist.append(varname)
            return ds[varlist].load()
    except Exception as e:
        raise RuntimeError(f'Reading the ACTRIS dataset failed: {dataset_id}') from e

if __name__ == "__main__":
    #print(get_list_platforms())
    #print(query_datasets_stations(['5qss']))
    dataset_id = "https://thredds.nilu.no/thredds/dodsC/ebas_doi/T6/Q7/ZC/T6Q7-ZCQY.nc"
    print(read_dataset(dataset_id, variables_list=['Aerosol Optical Properties']))
