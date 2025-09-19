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

from dataclasses import dataclass
import datetime
from numpy.typing import ArrayLike
from pathlib import Path
import pickle
import requests
import xarray as xr
import warnings

warnings.filterwarnings("ignore")

from icoscp_core.icos import meta, data, ATMO_STATION
from icoscp_core.metacore import StationTimeSeriesMeta


_SPARQL_ENDPOINT = 'https://meta.icos-cp.eu/sparql'

_MAPPING_ECV_ICOS = {
    'Pressure (surface)': ['AP'],
    'Surface Wind Speed and direction': ['WD', 'WS'],
    'Temperature (near surface)': ['AT'],
    'Water Vapour (surface)': ['RH'],
    'Carbon Dioxide, Methane and other Greenhouse gases': ['co2', 'ch4', 'n2o', 'co'],
    'Carbon Dioxide': ['co2'],
    'Methane': ['ch4'],
    'Nitrous Oxide': ['n2o'],
    'Carbon Monoxide': ['co']
}

_MAPPING_ICOS_VAR_TO_SPEC = {
    'AP': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'WD': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'WS': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'AT': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'RH': 'http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject',
    'co2': 'http://meta.icos-cp.eu/resources/cpmeta/ObspackTimeSerieResult',  # Alternatives: atcCo2Product / atcCo2L2DataObject
    'ch4': 'http://meta.icos-cp.eu/resources/cpmeta/ObspackCH4TimeSeriesResult',  # Alternatives: atcCh4Product / atcCh4L2DataObject
    'n2o': 'http://meta.icos-cp.eu/resources/cpmeta/ObspackN2oTimeSeriesResult',  # Alternatives: atcN2oProduct / atcN2oL2DataObject
    'co': 'http://meta.icos-cp.eu/resources/cpmeta/ObspackCOTimeSeriesResult'  # Alternatives: atcCoProduct / atcCoL2DataObject
}

_ECVS: set[str] = set(_MAPPING_ECV_ICOS.keys())

def _reverse_mapping(mapping: dict[str, list[str]]) -> dict[str, list[str]]:
    reversed_mapping: dict[str, list[str]] = {}
    for key, values in mapping.items():
        for value in values:
            reversed_mapping.setdefault(value, []).append(key)
    return reversed_mapping

_MAPPING_ICOS_ECV = _reverse_mapping(_MAPPING_ECV_ICOS)

_MAPPING_ECV_TO_ICOS_SPEC: dict[str, list[str]] = {}
for ecv, icos_vars in _MAPPING_ECV_ICOS.items():
    for icos_var in icos_vars:
        _MAPPING_ECV_TO_ICOS_SPEC.setdefault(ecv, []).append(_MAPPING_ICOS_VAR_TO_SPEC[icos_var])
# Remove duplicates.
for ecv, icos_specs in _MAPPING_ECV_TO_ICOS_SPEC.items():
    _MAPPING_ECV_TO_ICOS_SPEC[ecv] = list(set(icos_specs))

_MAPPING_ICOS_SPEC_TO_ECV = _reverse_mapping(_MAPPING_ECV_TO_ICOS_SPEC)

class TitleCache:
    def __init__(self, filename: Path, expire_time: int) -> None:
        self.filename: Path = filename
        self.expire_time: int = expire_time
        self.cache: dict[str, tuple[str, int]] = self.load()
    def load(self) -> dict[str, tuple[str, int]]:
        if self.filename.exists():
            with open(self.filename, 'rb') as cache_file:
                return pickle.load(cache_file)
        return {}
    def save(self) -> None:
        with open(self.filename, 'wb') as cache_file:
            pickle.dump(self.cache, cache_file)
    def get(self, dobj_url: str) -> str | None:
        if dobj_url in self.cache.keys():
            title, timestamp = self.cache[dobj_url]
            if datetime.datetime.now().timestamp() - timestamp > self.expire_time:
                self.cache.pop(dobj_url)
                self.save()
            else:
                return title
        return None
    def set(self, dobj_url: str, title: str) -> None:
        self.cache[dobj_url] = (title, int(datetime.datetime.now().timestamp()))
        self.save()

def _open_cache(path_to_cache: Path) -> None:
    global _TITLE_CACHE
    _TITLE_CACHE = TitleCache(path_to_cache, 3600*24)


@dataclass(frozen=True)
class Variable:
    name: str
    label: str | None
    unit: str | None


def get_list_platforms() -> list[dict[str, str | float | None]]:
    """
    Retrieves a list of ICOS platforms (stations). Each platform is described with a dictionary containing the keys:
    - 'short_name'
    - 'long_name'
    - 'longitude'
    - 'latitude'
    - 'altitude'
    - 'URI'
    e.g. {'short_name': 'HTM', 'long_name': 'Hyltemossa', 'longitude': 13.4189, 'latitude': 56.0976, 'altitude': 115.0, 'URI': 'http://meta.icos-cp.eu/resources/stations/AS_HTM'}
    :return: list of dict
    """

    stations: list[dict[str, str | float | None]] = []
    for station in meta.list_stations(ATMO_STATION):
        stations.append({
            'short_name': station.id,
            'long_name': station.name,
            'longitude': station.lon,
            'latitude': station.lat,
            'altitude': station.elevation,
            'URI': station.uri
        })
    return stations


def get_list_variables() -> list[dict[str, str | list[str]]]:
    """
    Retrieves a ICOS mapping between variable names and ECV names. The mapping is a list of dict. Each dict has
    the keys
    - 'variable_name'
    - 'ECV_name'
    and associates a single ICOS variable with one or many ECV name(s)
    e.g. {'variable_name': 'ch4', 'ECV_name': ['Methane', 'Carbon Dioxide, Methane and other Greenhouse gases']}
    :return: list of dict
    """
    return [
        {'variable_name': variable_name, 'ECV_name': ECV_names}
        for variable_name, ECV_names in _MAPPING_ICOS_ECV.items()
    ]


def query_datasets(
        codes: list[str],
        variables_list: list[str] | None=None,
        temporal_extent: list[str] | None=None
        ) -> list[dict[str, str | list[str] | list[dict[str, str]] | None]]:
    """
    Query the ICOS database for metadata of datasets satisfying the specified criteria.
    :param codes: a list of ICOS stations codes (short_name); selects datasets with platform_id in the list
    :param variables_list: optional; a list of ECV names; selects datasets with ecv_variables not disjoint with the list
    :param temporal_extent: optional; a list of the form [start_date, end_date]; start_date and end_date
    must match some ISO 8601 format; selects datasets with time_period overlapping temporal_extent interval
    :return: a list (possibly empty if no dataset satisfies the specified criteria) of dict with the keys:
    - 'title' (title of the dataset)
    - 'urls' (list of urls for the dataset, where the `type` of each url can be any of `landing_page`, `data_file`, `opendap`)
    - 'ecv_variables' (list of ecv variables included in the dataset)
    - 'time_period' (time period covered by the dataset)
    - 'platform_id' (id of the station, i.e. identical to `short_name` of the platform returned by method `get_list_platforms`)
    """
    # Keep only variables that are ECVs and list the corresponding ICOS data object specifications.
    selected_vars = _ECVS if variables_list is None else _ECVS.intersection(variables_list)
    specs: set[str] = set()
    for v in selected_vars:
        specs.update(_MAPPING_ECV_TO_ICOS_SPEC.get(v, []))

    # Check the validity of values in 'temporal_extent'.
    if temporal_extent is not None:
        if len(temporal_extent) != 2:
            raise ValueError('The temporal extent must include exactly two values or be None.')
        try:
            tstart, tend = map(lambda t: datetime.datetime.fromisoformat(t).isoformat(), temporal_extent)
            temporal_extent = [tstart, tend]
        except ValueError:
            raise ValueError('The temporal extent should be specified using ISO 8601 formats.')

    # Get metadata about data objects satisfying the specified criteria about ICOS data object specification,
    # stations of origin, and temporal extent.
    sparql_query = _sparql_query(list(specs), codes, temporal_extent)
    response = requests.post(
        _SPARQL_ENDPOINT,
        headers={'Accept': 'application/json'},
        data=bytes(sparql_query, 'utf-8'))
    if not response.ok:
        raise Exception(f'Something went wrong when querying the ICOS SPARQL endpoint with SPARQL query:\n{sparql_query}')
    dobjs_info: list[dict[str, dict[str, str]]] = response.json()['results']['bindings']

    outlist: list[dict[str, str | list[str] | list[dict[str, str]] | None]] = []
    for dobj in dobjs_info:
        title = _TITLE_CACHE.get(dobj['dobj']['value'])
        if title is None:
            dobj_meta = meta.get_dobj_meta(dobj['dobj']['value'])
            title = dobj_meta.references.title or ""
            _TITLE_CACHE.set(dobj['dobj']['value'], title)
        outlist.append({
            'title': title,
            'file_name': dobj['fileName']['value'],
            'urls': [{'url': dobj['dobj']['value'], 'type': 'landing_page'}],
            'ecv_variables': _MAPPING_ICOS_SPEC_TO_ECV[dobj['spec']['value']],
            'time_period': [dobj['timeStart']['value'], dobj['timeEnd']['value']],
            'platform_id': dobj['stationId']['value'],
        })
    return outlist


def _sparql_query(specs: list[str], station_ids: list[str], temporal_extent: list[str] | None):
    specs_str = f'<{"> <".join(specs)}>' if len(specs) > 0 else ''
    conditions_on_station_ids = [f'?stationId = "{s_id}"' for s_id in station_ids]
    station_ids_filter = ' || '.join(conditions_on_station_ids)
    if temporal_extent is None:
        temporal_filter_clause = ''
    else:
        temporal_filter_clause = (
            f'FILTER ( ?timeEnd >= "{temporal_extent[0]}"^^xsd:dateTime '
            f'&& ?timeStart <= "{temporal_extent[1]}"^^xsd:dateTime )'
        )
    return f"""
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
SELECT ?dobj ?spec ?fileName ?stationId ?timeStart ?timeEnd
WHERE {{
    VALUES ?spec {{ {specs_str} }}
    ?dobj cpmeta:hasObjectSpec ?spec .
    ?dobj cpmeta:hasSizeInBytes ?size .
    ?dobj cpmeta:hasName ?fileName .
    ?dobj cpmeta:wasAcquiredBy/prov:wasAssociatedWith/cpmeta:hasStationId ?stationId .
    ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
    ?dobj cpmeta:hasStartTime | (cpmeta:wasAcquiredBy / prov:startedAtTime) ?timeStart .
    ?dobj cpmeta:hasEndTime | (cpmeta:wasAcquiredBy / prov:endedAtTime) ?timeEnd .
    ?dobj cpmeta:hasVariableName ?varName .
    FILTER NOT EXISTS {{ [] cpmeta:isNextVersionOf ?dobj }}
    FILTER ( {station_ids_filter} )
    {temporal_filter_clause}
    {{
        {{ FILTER NOT EXISTS {{ ?dobj cpmeta:hasVariableName ?varName }} }}
        UNION
        {{ FILTER (?varName = "co2" || ?varName = "ch4" || ?varName = "co" || ?varName = "n2o" || ?varName = "RH" || ?varName = "WD" || ?varName = "WS") }}
    }}
}}
    """


def _read_dataset(dobj_uri: str) -> xr.Dataset:
    dobj_meta = meta.get_dobj_meta(dobj_uri)
    dobj_data = data.get_columns_as_arrays(dobj_meta)

    # Extract relevant attributes from metadata.
    variables: list[Variable] = []
    if isinstance(dobj_meta.specificInfo, StationTimeSeriesMeta):
        station = dobj_meta.specificInfo.acquisition.station
        sampling_height = dobj_meta.specificInfo.acquisition.samplingHeight
        # Extract relevant information about the variables (columns).
        if dobj_meta.specificInfo.columns is not None:
            for variable in dobj_meta.specificInfo.columns:
                # In Obspack netCDF data files, the column with gas concentration values is called 'value'.
                # The label of the valueType can be used to determine the gas species.
                if variable.label == 'value' and variable.valueType.self.label is not None:
                    variable_name = variable.valueType.self.label.split(' ')[0].lower()
                    # Replace the name of the 'value' column with the name of the gas species in the dictionary containing data.
                    dobj_data[variable_name] = dobj_data.pop('value')
                elif variable.label == 'TIMESTAMP':
                    variable_name = 'time'
                else:
                    variable_name = variable.label
                variables.append(
                    Variable(
                        name=variable_name,
                        label=variable.valueType.self.label,
                        unit=variable.valueType.unit))
    else:
        station = dobj_meta.specificInfo.station
        sampling_height = dobj_meta.specificInfo.samplingHeight
    if station is None:
        station_id = None
        station_name = None
        station_lon = None
        station_lat = None
        station_alt = None
    else:
        station_id = station.id
        station_name = station.org.name
        if station.location is None:
            station_lon = None
            station_lat = None
            station_alt = None
        else:
            station_lon = station.location.lon
            station_lat = station.location.lat
            station_alt = station.location.alt

    # Some datasets have a "time" variable and other have a "TIMESTAMP" variable.
    # Use the "time" label everywhere.
    if 'TIMESTAMP' in dobj_data.keys():
        dobj_data['time'] = dobj_data.pop('TIMESTAMP')
    if 'time' not in dobj_data.keys():
        raise RuntimeError(f'ICOS dataset "{dobj_uri}" does not contain any "time" or "TIMESTAMP" variable.')

    # Restructure the dictionary to match the structure requested by xarray.Dataset.from_dict.
    dobj_data_formatted: dict[str, dict[str, ArrayLike]] = {
        var: {'dims': ('time'), 'data': vals} for var, vals in dobj_data.items()}

    dataset = xr.Dataset.from_dict(dobj_data_formatted)
    dataset.attrs = {
        'title': dobj_meta.references.title,
        'station_id': station_id,
        'station_name': station_name,
        'station_lon': station_lon,
        'station_lat': station_lat,
        'station_alt': station_alt,
        'sampling_height': sampling_height
    }
    for variable in variables:
        dataset[variable.name] = dataset[variable.name].assign_attrs({
            'label': variable.label, 'units': variable.unit
        })

    return dataset


def read_dataset(dataset_id: list[dict[str, str]], variables_list: list[str] | None=None):
    """
    Retrieves a dataset identified by dataset_id and selects variables listed in variables_list.
    :param dataset_id: list of dict with keys 'url', 'type', e.g.
    [{'url': 'https://meta.icos-cp.eu/objects/ajg7CfaO7d1S_PT5IlHwS9SN', 'type': 'landing_page'}]
    :param variables_list: list of str, optional; a list of ECV names
    :return: xarray.Dataset object with at least one variable, otherwise returns None
    """
    # Keep only variables that are ECVs and list the corresponding ICOS variables.
    selected_vars = _ECVS if variables_list is None else _ECVS.intersection(variables_list)
    icos_variables: set[str] = set()
    for v in selected_vars:
        icos_variables.update(_MAPPING_ECV_ICOS.get(v, []))

    # Go through all the URLs provided for the dataset and use the one corresponding to a landing page to fetch the data.
    for url_info in dataset_id:
        url = url_info['url']
        url_type = url_info['type']
        if url_type.lower() != 'landing_page':
            continue

        try:
            ds = _read_dataset(url)
        except Exception as e:
            raise RuntimeError(f'Reading the ICOS dataset failed: {dataset_id}') from e

        # Filter the dataset to keep only the requested variables.
        variables: set[str] = set([str(v) for v in ds.data_vars]).intersection(icos_variables)
        if len(variables) == 0:
            # maybe the next url (if any) will work better...
            continue
        ds = ds[variables]
        return ds
    else:
        raise RuntimeError(f'No data or no ECV was found in ICOS dataset: {dataset_id}')


if __name__ == "__main__":
    stations = get_list_platforms()
    codes = [station['short_name'] for station in stations[5:10]]
    info_datasets = query_datasets(codes, variables_list=['Temperature (near surface)', 'Nitrous Oxide'], temporal_extent=['2023-01-01T00:00Z', '2024-04-30'])
    #info_datasets = query_datasets(codes, variables_list=['Pressure (surface)'], temporal_extent=['2018-01-01T03:00:00','2021-12-31T23:00:00'])
    #info_datasets = query_datasets(codes, variables_list=['Carbon Dioxide', 'Surface Wind Speed and direction','Carbon Dioxide, Methane and other Greenhouse gases'], temporal_extent=['2018-01-01T03:00:00','2021-12-31T23:00:00'])
    dataset = read_dataset(info_datasets[0]['urls'])
    print(f'Stations included in search:\n{codes}')
    print(f'\n\nInformation about the datasets that were found:\n{info_datasets}')
    print(f'\n\nFirst rows of the first dataset:\n{dataset}')