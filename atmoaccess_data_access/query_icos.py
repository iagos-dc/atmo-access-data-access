'''
Provide Atmospheric stations, variables and datasets
for EnvriFair Task 8.5
API description: https://docs.google.com/document/d/1_YjLJQqO4ZPoIkPgvSKkhJzlUPptIEm0F1X7uDrKRiA/edit
The corresponding Essential Climate Variables for ICOS are:
https://gcos.wmo.int/en/essential-climate-variables/surface-vapour/ecv-requirements
https://gcos.wmo.int/en/essential-climate-variables/surface-temperature/ecv-requirements
https://gcos.wmo.int/en/essential-climate-variables/surface-wind/ecv-requirements
https://gcos.wmo.int/en/essential-climate-variables/pressure/ecv-requirements
https://gcos.wmo.int/en/essential-climate-variables/ghg/ecv-requirements
'''

import functools
import pandas as pd
import xarray as xr
import warnings

warnings.filterwarnings("ignore")

from icoscp.station import station
from icoscp.dobj import Dobj
from icoscp.sparql.runsparql import RunSparql

from atmoaccess_data_access import helper

# from config import ICOSCP_TOKEN
# from icoscp.cpauth.authentication import Authentication
# Authentication(token=ICOSCP_TOKEN)


_ICOS_VAR_TO_ECV_MAPPING = {
    'AP': [
        'Pressure (surface)',
        'Pressure',
        'ap'
    ],
    'WD': [
        'Surface Wind Speed and direction',
        'wd'
    ],
    'WS': [
        'Surface Wind Speed and direction',
        'ws'
    ],
    'AT': [
        'Temperature (near surface)',
        'Temperature',
        'at'
    ],
    'RH': [
        'Water Vapour (surface)',
        'Water Vapour (Relative Humidity)',
        'rh'
    ],
    'co2': [
        'Carbon Dioxide',
        'Carbon Dioxide, Methane and other Greenhouse gases',
        'Tropospheric CO2',
        'co2'
    ],
    'co': [
        'Carbon Monoxide',
        'Carbon Dioxide, Methane and other Greenhouse gases',
        'co'
    ],
    'ch4': [
        'Methane',
        'Carbon Dioxide, Methane and other Greenhouse gases',
        'Tropospheric CH4',
        'ch4'
    ],
    'n2o': [
        'Nitrous Oxide',
        'Carbon Dioxide, Methane and other Greenhouse gases',
        'n2o'
    ]
}

_ICOS_VAR_lowercase_TO_ECV_MAPPING = {k.lower(): v for k, v in _ICOS_VAR_TO_ECV_MAPPING.items()}

_ECV_TO_ICOS_VARS_lowercase = {}
for icos_var, ecvs in _ICOS_VAR_lowercase_TO_ECV_MAPPING.items():
    for ecv in ecvs:
        _ECV_TO_ICOS_VARS_lowercase.setdefault(ecv, []).append(icos_var)

_ICOS_VAR_lowercase_TO_SPEC = {
    'ap': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'wd': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'ws': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'at': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'rh': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'co2': 'http://meta.icos-cp.eu/resources/cpmeta/atcCo2Product',  # 'http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject',
    'co': 'http://meta.icos-cp.eu/resources/cpmeta/atcCoL2DataObject',
    'ch4': 'http://meta.icos-cp.eu/resources/cpmeta/atcCh4Product',  # 'http://meta.icos-cp.eu/resources/cpmeta/atcCh4L2DataObject',
    'n2o': 'http://meta.icos-cp.eu/resources/cpmeta/atcN2oL2DataObject'
}

_SPEC_TO_ICOS_VAR_lowercase = {}
for k, v in _ICOS_VAR_lowercase_TO_SPEC.items():
    _SPEC_TO_ICOS_VAR_lowercase.setdefault(v, []).append(k)


# all stations info
def get_list_platforms():
    '''
    Query ICOS for a list of atmosperic stations
    Returns
    -------
    stations : LIST[dicts]
    '''

    stations = station.getIdList(project='all')

    # remove ecosystem and ocean for this demonstrator
    # but, stations would contain ALL stations from ICOS
    stations = stations[stations['theme'] == 'AS']
    # make projection and rename columns to conform
    col_dict = _colname()
    stations = stations[list(col_dict)].rename(columns=col_dict)
    for col in ['latitude', 'longitude', 'altitude']:
        stations[col] = pd.to_numeric(stations[col])
    # transform to desired output format
    stations = stations.to_dict(orient='records')

    return stations


def _colname():
    # rename columns for compatibility
    colname = {
        'id': 'short_name',
        'name': 'long_name',
        'lat': 'latitude',
        'lon': 'longitude',
        'elevation': 'altitude',
        'uri': 'URI',
    }
    return colname


@functools.cache
def get_list_variables():
    """
    Return a list of Variables from ICOS for the moment this is a
    fixed dictionary, but could / should be dynamically queried
    Returns
    -------
    variables : LIST[dicts]
    """
    variables = [
        {'variable_name': variable_name, 'ECV_name': ECV_names}
        for variable_name, ECV_names in _ICOS_VAR_TO_ECV_MAPPING.items()
    ]
    return variables


@functools.cache
def ecv_icos_map():
    rev_mapping = {}
    for var in get_list_variables():
        variable_name = var['variable_name']
        ECV_names = var['ECV_name']
        for ECV_name in ECV_names:
            rev_mapping.setdefault(ECV_name, []).append(variable_name)
    return rev_mapping


@functools.cache
def __get_spec(variable_name):
    """
    Return a list of Variables from ICOS for the moment this is a
    fixed dictionary, but could / should be dynamically queried
    Returns
    -------
    variables : LIST[dicts]
    """
    # make sure variable_name is lower case
    variable_name = variable_name.lower()
    return _ICOS_VAR_lowercase_TO_SPEC.get(variable_name, '')


@functools.cache
def __get_ecv(spec):
    icos_vars_lowercase = _SPEC_TO_ICOS_VAR_lowercase.get(spec, [])
    ecvs = set()
    for v in icos_vars_lowercase:
        ecvs.update(_ICOS_VAR_lowercase_TO_ECV_MAPPING[v])
    return list(ecvs)


def query_datasets(codes, variables_list=None, temporal_extent=None):
    """
    return identifiers for datasets constraint by input parameters.
    if a parameter is empty, it will be ignored. if no parameter is
    provided...ALL datasets are returned.
    Parameters
    ----------
    codes : LIST[STR]
        A list of station codes.
    variables_list : LIST[STR]
        Provide a list of strings to query for variables. Entries\
        matching the variables returned from get_list_variables().
    temporal_extent : LIST[STR,STR] start, end , string at format yyyyMMddTHHmmss
                or more general str must be convertible with a pandas.
                date = pandas.to_datetime(date).date()
    Returns
    -------
    LIST[DICT]
    Where DICT is of form {title:’’, urls:[{url:’’, type:”}], ecv_variables:[], time_period:[start, end], platform_id:””}
    title: title of the dataset
    urls: list of urls for the dataset. Can include link to a landing page, link to the data file, opendap link
    url.type: type of the url. Should be in list: landing_page, data_file, opendap
    ecv_variables: list of ecv variables included in the dataset
    time_period: time period covered by the dataset
    platform_id: id of the station (i.e. identical to short_name of the platform return by method get_list_platforms())
    If there are no results an empty list is returned
    """
    stn = station.getIdList(project='all')
    stn = stn[(stn['theme'] == 'AS')] #& (stn['icosClass'].isin(['1', '2', 'Associated']))]
    dtypes = ['str', 'str', 'str', 'str', 'float', 'float', 'float', 'str', 'str']
    dtype = dict(zip(stn.columns.tolist(), dtypes))
    # get all datasets and convert dtype
    dataset = __sparql_data()
    dtypes = ['str', 'str', 'str', 'str', 'int', 'datetime64[ns, UTC]', 'datetime64[ns, UTC]', 'datetime64[ns, UTC]']
    dtype = dict(zip(dataset.columns.tolist(), dtypes))
    dataset = dataset.astype(dtype)

    # add platform_id
    dataset['platform_id'] = dataset['station'].str.slice(-3)

    # filter on platform codes
    filter_on_codes = dataset['platform_id'].isin(set(codes))
    dataset = dataset[filter_on_codes]
    if dataset.empty:
        # no dataset correspond to codes
        return []

    # start filtering according to variables_list
    if variables_list is None:
        variables_list = list(_ECV_TO_ICOS_VARS_lowercase)

    selected_var = []
    for vv in get_list_variables():
        # convert all to lowercase, for more resilience
        ecv = [v.lower() for v in vv['ECV_name']]
        for v in variables_list:
            if v.lower() in ecv:
                selected_var.append(vv['variable_name'])

    # make sure there are no duplicates
    selected_var = list(set(selected_var))

    # filter provided variables from all datasets
    _dfs = []
    for v in selected_var:
        data = dataset[dataset['spec'] == __get_spec(v)]
        if not data.empty:
            # add only non-empty data to the list
            _dfs.append(data)

    if len(_dfs) == 0:
        return []
    df = pd.concat(_dfs)

    # make sure there are no duplicates (meteo variables are in the same file)
    df = df.drop_duplicates(subset=['dobj'])

    # filter temporal
    if temporal_extent is not None and len(temporal_extent) == 2:
        t0, t1 = map(lambda t: pd.to_datetime(t, utc=True), temporal_extent)
        df = df[(df.timeStart <= t1) & (df.timeEnd >= t0)]

    if df.empty:
        return []

    df = df.reset_index(drop=True)

    # transfrom pandas dataframe to dict to conform for envri
    # fair demonstrator

    outlist = []
    import data_processing  # TODO: get rid of this "circular" dependency! implement internal caching, as for ACTRIS
    for r in df.iterrows():
        d = {
            'title': data_processing.GetICOSDatasetTitleRequest(r[1].dobj).compute(),
            'file_name': r[1].fileName,
            'urls': [{'url': r[1].dobj, 'type': 'landing_page'}],
            'ecv_variables': __get_ecv(r[1].spec),
            'time_period': [r[1].timeStart, r[1].timeEnd],
            'platform_id': r[1].platform_id,
        }
        outlist.append(d)
    return outlist


def __sparql_data():
    q = """	   prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
     prefix prov: <http://www.w3.org/ns/prov#>
    	select ?station ?dobj ?spec ?fileName ?size ?submTime ?timeStart ?timeEnd
    	where {
        		VALUES ?spec {
        			<http://meta.icos-cp.eu/resources/cpmeta/atcCh4Product> 
        			<http://meta.icos-cp.eu/resources/cpmeta/atcCoL2DataObject>
        			<http://meta.icos-cp.eu/resources/cpmeta/atcCo2Product>
        			<http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject>
        			<http://meta.icos-cp.eu/resources/cpmeta/atcN2oL2DataObject>}
        	?dobj cpmeta:hasObjectSpec ?spec .
        	?dobj cpmeta:hasSizeInBytes ?size .
        	?dobj cpmeta:hasName ?fileName .
        	?dobj cpmeta:wasAcquiredBy/prov:wasAssociatedWith ?station .
        	?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
        	?dobj cpmeta:hasStartTime | (cpmeta:wasAcquiredBy / prov:startedAtTime) ?timeStart .
        	?dobj cpmeta:hasEndTime | (cpmeta:wasAcquiredBy / prov:endedAtTime) ?timeEnd .
        	FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
        	{
        		{FILTER NOT EXISTS {?dobj cpmeta:hasVariableName ?varName}}
        		UNION
        		{
        			?dobj cpmeta:hasVariableName ?varName
        			FILTER (?varName = "co2" || ?varName = "ch4" || ?varName = "co" || ?varName = "n2o" || ?varName = "RH" || ?varName = "WD" || ?varName = "WS")
        		}
        	}
        }
    """
    sparql = RunSparql(q, output_format='pandas').run()

    return sparql


def _read_dataset(pid):
    digital_object = Dobj(pid)
    # Get data & meta-data of the digital object.
    data_df, meta_data = digital_object.data, digital_object.info
    # In case of empty data or meta-data return an empty dataset.
    if data_df is None or meta_data is None:
        return None
    # Initiate a dataset from the `data_df` dataframe.
    dataset = xr.Dataset.from_dataframe(data_df)

    # setup global attributes
    dobj_station = getattr(digital_object, 'station', None)
    dataset.attrs = {
        'title': helper.getkeyvals(meta_data, 'references', 'title'),
        'station_id': helper.getkeyvals(dobj_station, 'id'),
        'station_name': helper.getkeyvals(dobj_station, 'org', 'name'),
        'station_lon': getattr(digital_object, 'lon', None),
        'station_lat': getattr(digital_object, 'lat', None),
        'station_alt': getattr(digital_object, 'alt', None),
        'sampling_height': helper.getkeyvals(meta_data, 'specificInfo', 'acquisition', 'samplingHeight')
    }

    # Loop over the variables in the meta-data.
    for variable_dict in meta_data['specificInfo']['columns']:
        attributes = dict()
        variable_name = variable_dict['label']
        # Extract 'label' meta-data.
        attributes['label'] = variable_dict['valueType']['self']['label']
        # Some variables do not come with units.
        if 'unit' in variable_dict['valueType'].keys():
            # Extract 'units' meta-data.
            attributes['units'] = variable_dict['valueType']['unit']
        # Update the variables' attributes of the initialized dataset
        # in-place. Each extracted meta-data value will be a new
        # attribute under the corresponding variable.
        dataset[variable_name] = dataset[variable_name].assign_attrs(attributes)
    return dataset


def read_dataset(dataset_id, variables_list=None):
    """
    Retrieves a dataset identified by dataset_id and selects variables listed in variables_list.
    :param dataset_id: list of dict with keys 'url', 'type', e.g.
    [{'url': 'https://meta.icos-cp.eu/objects/ajg7CfaO7d1S_PT5IlHwS9SN', 'type': 'landing_page'}]
    :param variables_list: list of str, optional; a list of ECV names
    :return: xarray.Dataset object with at least one variable, otherwise returns None
    """
    if variables_list is None:
        variables_set = set(_ECV_TO_ICOS_VARS_lowercase)
    else:
        variables_set = set(variables_list).intersection(_ECV_TO_ICOS_VARS_lowercase)
    icos_variables_lowercase = set(sum((_ECV_TO_ICOS_VARS_lowercase[v] for v in variables_set), start=[]))

    try:
        for url_type_dict in dataset_id:
            pid = url_type_dict['url']
            typ = url_type_dict['type']
            if typ.lower() != 'landing_page':
                continue
            ds = _read_dataset(pid)
            if ds is None:
                # maybe the next url (if any) will work better...
                continue

            # re-index the dataset using 'TIMESTAMP' variable and rename it to 'time'
            ds = ds.set_coords('TIMESTAMP')\
                    .swap_dims({'index': 'TIMESTAMP'})\
                    .reset_coords('index', drop=True)\
                    .rename({'TIMESTAMP': 'time'})

            # filter according to variables_list
            vs = list(ds)
            vs = [v for v in vs if v.lower() in icos_variables_lowercase]
            if not vs:
                # maybe the next url (if any) will work better...
                continue
            ds = ds[vs]
            return ds.load()

    except Exception as e:
        raise RuntimeError(f'Reading the ICOS dataset failed: {dataset_id}') from e
    return None


if __name__ == "__main__":
    a = get_list_platforms()
    codes = [platform['short_name'] for platform in a[5:50]]
    # print(get_list_variables())
    b = query_datasets(codes, variables_list=['temperature', 'n2o'], temporal_extent=['2016-01-01T00:00Z', '2016-04-30'])
    print(b)
    # print(query_datasets(variables_list=['Pressure (surface)'], temporal_extent=['2018-01-01T03:00:00','2021-12-31T24:00:00']))
    # pids = query_datasets(variables_list=['co2', 'ws','Carbon Dioxide, Methane and other Greenhouse gases'], temporal_extent=['2018-01-01T03:00:00','2021-12-31T24:00:00'])
    # print(pids)
    # data = read_dataset(pids[0])
    # print(data.head())