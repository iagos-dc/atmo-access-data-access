import logging
import warnings
import io
import diskcache
import requests
from requests.exceptions import HTTPError
import pandas as pd
import xarray as xr


logger = logging.getLogger(__name__)


ALLOW_READ_DATASET_VIA_HTTP = True  # indicates if the routine read_dataset can fallback to reading via http if reading via opendap failed
_CACHE_EXPIRE_TIME = 3600 * 36  # cache expires in 36h

REST_URL_PATH = "https://prod-actris-md2.nilu.no/"
REST_URL_STATIONS = REST_URL_PATH + "facilities"
REST_URL_VARIABLES = REST_URL_PATH + "vocabulary/contentattribute"
REST_URL_DOWNLOAD = REST_URL_PATH + "metadata/"

STATIC_PARAMETERS = ["latitude", "longitude", "air_pressure", "barometric_altitude", "pressure", "relative_humidity", "temperature"]

MAPPING_ECV_ACTRIS = {'Aerosol Optical Properties': ['aerosol particle light absorption coefficient', 'aerosol particle light hemispheric backscatter coefficient', 'aerosol particle light scattering coefficient', 'aerosol particle optical depth'], 'Aerosol Chemical Properties': ['aerosol particle elemental carbon mass concentration', 'aerosol particle organic carbon mass concentration'], 'Aerosol Physical Properties': ['aerosol particle number concentration', 'cloud condensation nuclei number concentration', 'aerosol particle number size distribution', 'cloud condensation nuclei number size distribution'], 'NO2': ['nitrogen dioxide amount fraction', 'nitrogen dioxide mass concentration']}

MAPPING_EBAS_ACTRIS = {
    'aerosol_light_backscattering_coefficient': ['aerosol particle light hemispheric backscatter coefficient'],
    'aerosol_light_scattering_coefficient': ['aerosol particle light scattering coefficient'],
    'aerosol_absorption_coefficient': ['aerosol particle light absorption coefficient'],
    'aerosol_optical_depth': ['aerosol particle optical depth'],
    'elemental_carbon': ['aerosol particle elemental carbon mass concentration'],
    'organic_carbon': ['aerosol particle organic carbon mass concentration'],
    'particle_number_concentration': ['aerosol particle number concentration'],
    'cloud_condensation_nuclei_number_concentration': ['cloud condensation nuclei number concentration'],
    'particle_number_size_distribution': ['aerosol particle number size distribution'],
    'cloud_condensation_nuclei_number_size_distribution': ['cloud condensation nuclei number size distribution'],
    'nitrogen_dioxide': ['nitrogen dioxide amount fraction', 'nitrogen dioxide mass concentration'],
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


_cache = None

def _open_cache(url):
    import diskcache
    global _cache
    _cache = diskcache.Cache(directory=url)


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


def _transform_dataset(dataset, variable):
    """
    Transforms ACTRIS dataset's metadata into ATMO-ACCESS-specific metadata. The latter includes 'ecv_variables',
    which indicates which ECV are present in the dataset. If none of the ECV is present, this function returns None.
    :param dataset: dict
    :param variable: str with the name of an ACTRIS variable
    :return: dict or None
    """
    title = dataset['md_identification']['title']
    md_distribution_information = dataset['md_distribution_information']
    dataset_urls = [
        {'url': entry['dataset_url'], 'type': entry['protocol']}
        for entry in md_distribution_information
        if entry['protocol'] in ['OPeNDAP', 'HTTP']
    ]
    ecvs = MAPPING_ACTRIS_ECV.get(variable, [])
    ecvs_present = read_dataset(dataset_urls, variables_list=ecvs, get_ECV_only=True)
    if not ecvs_present:
        return None
    time_period = [dataset['ex_temporal_extent']['time_period_begin'], dataset['ex_temporal_extent']['time_period_end']]
    platform_id = dataset['md_data_identification']['facility']['identifier']
    return {
        '_id': dataset['id'],
        'title': title,
        'urls': dataset_urls,
        'ecv_variables': ecvs_present,
        'time_period': time_period,
        'platform_id': platform_id
    }


def _query_datasets_for_station_and_variable(code, variable):
    page = 0
    _all_datasets = []
    while True:
        try:
            url = f"{REST_URL_PATH}/metadata/facility/{code}/content/{variable}/page/{page}"
            response = requests.get(url)
            response.raise_for_status()
            datasets = response.json()

            if not datasets:
                break

            _all_datasets.extend(datasets)
            page += 1
        except HTTPError as http_err:
            warnings.warn(
                f'HTTP error occurred while querying ACTRIS datasets for station={code}, page={page}: {http_err}')
            #raise
        except Exception as err:
            warnings.warn(f'Other error occurred while querying ACTRIS datasets for station={code}, page={page}: {err}')
            #raise

    all_datasets = []
    for _ds in _all_datasets:
        try:
            _id = 'unknown'
            _id = _ds.get('id')
            ds = _transform_dataset(_ds, variable)
            if ds is not None:
                all_datasets.append(ds)
        except Exception as e:
            logger.exception(f'_transform_dataset failed for dataset with id={_id}', exc_info=e)
    return all_datasets


def query_datasets_for_station_and_variable(code, variable):
    res = None
    if _cache is not None:
        res = _cache.get((code, variable))
    if res is None:
        res = _query_datasets_for_station_and_variable(code, variable)
        if _cache is not None:
            _cache.set((code, variable), res, expire=_CACHE_EXPIRE_TIME)
    return res


def _create_cache_and_close(url):
    cache = diskcache.Cache(directory=url)
    station_codes = list(map(lambda platform: platform['short_name'], get_list_platforms()))
    all_variables = list(MAPPING_ACTRIS_ECV)
    for code in station_codes:
        for variable in all_variables:
            res = _query_datasets_for_station_and_variable(code, variable)
            cache.set((code, variable), res, expire=_CACHE_EXPIRE_TIME)
        logger.info(f'station {code}...done')
    cache.close()


def query_datasets_stations(codes, variables_list=None, temporal_extent=None):
    """
    Query the ACTRIS database for metadata of datasets satisfying the specified criteria.
    """
    if variables_list is None:
        variables_list = list(MAPPING_ECV_ACTRIS)
    else:
        variables_list = [var for var in variables_list if var in MAPPING_ECV_ACTRIS]

    variables_to_query = set(var for variable in variables_list for var in MAPPING_ECV_ACTRIS[variable])
    _all_datasets = []

    for code in codes:
        for variable in variables_to_query:
            _datasets = query_datasets_for_station_and_variable(code, variable)
            _all_datasets.extend(_datasets)

    # remove duplicates and filter on temporal_extent, if needed
    if temporal_extent is not None:
        t0, t1 = map(pd.to_datetime, temporal_extent)
    all_ids = set()
    filtered_datasets = []
    for dataset in _all_datasets:
        i = dataset['_id']
        if i in all_ids:
            continue
        all_ids.add(i)
        if temporal_extent is not None:
            time_period = dataset['time_period']
            if pd.to_datetime(time_period[0]) > t1 or pd.to_datetime(time_period[1]) < t0:
                continue
        del dataset['_id']
        filtered_datasets.append(dataset)

    return filtered_datasets


def read_dataset(dataset_id, variables_list=None, get_ECV_only=False):
    """
    Retrieves a dataset identified by dataset_id and selects variables listed in variables_list.
    :param dataset_id: str; an identifier (e.g. an url) of the dataset to retrieve
    :param variables_list: list of str, optional; a list of ECV names
    :return: xarray.Dataset object
    """
    if variables_list is None:
        variables_list = list(MAPPING_ECV_ACTRIS)
    variables_set = set(variables_list)

    url_opendap = None
    url_http = None
    if isinstance(dataset_id, str):
        url_opendap = dataset_id
        url_http = dataset_id
    else:
        # dataset_id is a list like:
        # [{'url': 'https://thredds.nilu.no/thredds/fileServer/ebas_doi/BT/VX/4A/BTVX-4AVV.nc','type': 'HTTP'},
        # {'url': 'https://thredds.nilu.no/thredds/dodsC/ebas_doi/BT/VX/4A/BTVX-4AVV.nc', 'type': 'OPeNDAP'}]
        try:
            for url_dict in dataset_id:
                if url_dict['type'] == 'OPeNDAP':
                    url_opendap = url_dict['url']
                elif url_dict['type'] == 'HTTP':
                    url_http = url_dict['url']
        except Exception as e:
            raise ValueError(f'invalid dataset_id; should be an url or a list of dict with keys "url" and "type"; got dataset_id={dataset_id}') from e

    def filter_ds_vars(ds):
        # remove all trivial, single-coordinate dimensions
        ds = ds.squeeze(drop=True)

        # remove all variables having dimension(s) other than 'time'
        dims_other_than_time = [d for d in ds.dims if d != 'time']
        if dims_other_than_time:
            ds = ds.drop_dims(dims_other_than_time)

        varlist = []
        ecv_names_for_all_vars = []
        for varname, da in ds.data_vars.items():
            if any(pattern in varname for pattern in ['uncertainty', 'prec1587', 'perc8413', 'stddev', 'min', 'max', 'ExpUnc2s', 'det.lim.', 'precision', 'size_distribution', 'coefficient_amean_']):
                continue
            if da.attrs.get('ebas_statistics') == 'uncertainty':
                continue
            if 'ebas_component' not in da.attrs:
                continue
            ebas_name = da.attrs['ebas_component']
            if ebas_name in STATIC_PARAMETERS:
                continue
            actris_names = MAPPING_EBAS_ACTRIS.get(ebas_name)
            if actris_names is None:
                continue
            ecv_names = []
            for actris_name in actris_names:
                _ecv_names = MAPPING_ACTRIS_ECV.get(actris_name, [])
                ecv_names.extend(_ecv_names)
            if variables_set.isdisjoint(ecv_names):
                continue
            varlist.append(varname)
            ecv_names_for_all_vars.extend(ecv_names)
        ecv_names_for_all_vars = sorted(set(ecv_names_for_all_vars))
        if varlist:
            if get_ECV_only:
                ds_filtered = None
            else:
                ds_filtered = ds[varlist].load()
        else:
            ds_filtered = None
        return ds_filtered, ecv_names_for_all_vars

    try:
        assert url_opendap is not None
        with xr.open_dataset(url_opendap) as ds:
            ds_filtered, ecv_names_for_all_vars = filter_ds_vars(ds)
    except Exception as e:
        if ALLOW_READ_DATASET_VIA_HTTP and url_http is not None:
            try:
                response = requests.get(url_http)
                response.raise_for_status()
                with io.BytesIO(response.content) as buf:
                    with xr.open_dataset(buf, engine='h5netcdf') as ds:
                        ds_filtered, ecv_names_for_all_vars = filter_ds_vars(ds)
            except Exception as e:
                raise RuntimeError(f'Reading the ACTRIS dataset failed: {dataset_id}') from e
        else:
            raise RuntimeError(f'Reading the ACTRIS dataset failed: {dataset_id}') from e

    if get_ECV_only:
        return ecv_names_for_all_vars
    else:
        return ds_filtered


if __name__ == "__main__":
    #print(get_list_platforms())
    #print(query_datasets_stations(['5qss']))
    dataset_id = "https://thredds.nilu.no/thredds/dodsC/ebas_doi/T6/Q7/ZC/T6Q7-ZCQY.nc"
    print(read_dataset(dataset_id, variables_list=['Aerosol Optical Properties']))
