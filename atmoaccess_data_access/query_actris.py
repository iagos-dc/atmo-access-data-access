import warnings
import io
import requests
from requests.exceptions import HTTPError
import pandas as pd
import xarray as xr

REST_URL_PATH = "https://dev-actris-md2.nilu.no/"
REST_URL_STATIONS = REST_URL_PATH + "facilities"
REST_URL_VARIABLES = REST_URL_PATH + "vocabulary/contentattribute"
REST_URL_DOWNLOAD = REST_URL_PATH + "metadata/"

STATIC_PARAMETERS = ["latitude", "longitude", "air_pressure", "barometric_altitude"]

MAPPING_ECV_ACTRIS = {'Aerosol Optical Properties': ['aerosol particle light absorption coefficient', 'aerosol particle backscatter coefficient', 'aerosol particle light extinction coefficient', 'aerosol particle light hemispheric backscatter coefficient', 'aerosol particle light scattering coefficient', 'aerosol particle optical depth'], 'Aerosol Chemical Properties': ['aerosol particle aluminium mass concentration', 'aerosol particle ammonium mass concentration', 'aerosol particle arsenic mass concentration', 'aerosol particle barium mass concentration', 'aerosol particle bromine mass concentration', 'aerosol particle cadmium mass concentration', 'aerosol particle calcium mass concentration', 'aerosol particle carbonate carbon mass concentration', 'aerosol particle carbon mass concentration', 'aerosol particle chloride mass concentration', 'aerosol particle chlorine mass concentration', 'aerosol particle chromium mass concentration', 'aerosol particle cobalt mass concentration', 'aerosol particle copper mass concentration', 'aerosol particle elemental carbon mass concentration', 'aerosol particle equivalent black carbon mass concentration', 'aerosol particle iron mass concentration', 'aerosol particle lead mass concentration', 'aerosol particle magnesium mass concentration', 'aerosol particle manganese mass concentration', 'aerosol particle mass concentration', 'aerosol particle mercury mass concentration', 'aerosol particle molybdenum mass concentration', 'aerosol particle nickel mass concentration', 'aerosol particle nitrate mass concentration', 'aerosol particle organic carbon mass concentration', 'aerosol particle organics mass concentration', 'aerosol particle phosphorus mass concentration', 'aerosol particle potassium mass concentration', 'aerosol particle rubidium mass concentration', 'aerosol particle scandium mass concentration', 'aerosol particle selenium mass concentration', 'aerosol particle silicon mass concentration', 'aerosol particle sodium mass concentration', 'aerosol particle strontium mass concentration', 'aerosol particle sulfur mass concentration', 'aerosol particle titanium mass concentration', 'aerosol particle total carbon mass concentration', 'aerosol particle vanadium mass concentration', 'aerosol particle zinc mass concentration', 'aerosol particle zirconium mass concentration', 'aerosol particle sulphate mass concentration', 'aerosol particle iron 57 mass concentration', 'aerosol particle benzo(b)naphtho(2,1-d)thiophene mass concentration', 'aerosol particle levoglucosan mass concentration', 'aerosol particle indeno(1,2,3-cd)pyrene mass concentration', 'aerosol particle octabromo-1,3,3-trimethyl-1-phenylindane mass concentration', 'aerosol particle 1,2,3,4,5-pentabromobenzene mass concentration', 'aerosol particle dechlorane plus anti mass concentration', 'aerosol particle benzo(c)phenanthrene mass concentration', 'aerosol particle benzo(e)pyrene mass concentration', 'aerosol particle benzo(ghi)perylene mass concentration', 'aerosol particle benzo(j)fluoranthene mass concentration', 'aerosol particle benzo(k)fluoranthene mass concentration', 'aerosol particle (Z)-nonachlor mass concentration', 'aerosol particle cyclopenta(cd)pyrene mass concentration', 'aerosol particle methanesulfonate mass concentration', 'aerosol particle dechlorane plus syn mass concentration', 'aerosol particle PCB 1 mass concentration', 'aerosol particle PCB 101 mass concentration', 'aerosol particle PCB 105 mass concentration', 'aerosol particle PCB 110 mass concentration', 'aerosol particle PCB 114 mass concentration', 'aerosol particle PCB 118 mass concentration', 'aerosol particle PCB 119 mass concentration', 'aerosol particle PCB 123 mass concentration', 'aerosol particle PCB 126 mass concentration', 'aerosol particle PCB 128 mass concentration', 'aerosol particle PCB 131 mass concentration', 'aerosol particle PCB 132 mass concentration', 'aerosol particle PCB 134 mass concentration', 'aerosol particle PCB 136 mass concentration', 'aerosol particle PCB 137 mass concentration', 'aerosol particle PCB 138 mass concentration', 'aerosol particle PCB 141 mass concentration', 'aerosol particle PCB 146 mass concentration', 'aerosol particle PCB 149 mass concentration', 'aerosol particle PCB 15 mass concentration', 'aerosol particle PCB 151 mass concentration', 'aerosol particle PCB 153 mass concentration', 'aerosol particle PCB 156 mass concentration', 'aerosol particle PCB 157 mass concentration', 'aerosol particle PCB 158 mass concentration', 'aerosol particle PCB 167 mass concentration', 'aerosol particle PCB 169 mass concentration', 'aerosol particle PCB 17 mass concentration', 'aerosol particle PCB 170 mass concentration', 'aerosol particle PCB 171 mass concentration', 'aerosol particle PCB 174 mass concentration', 'aerosol particle PCB 175 mass concentration', 'aerosol particle PCB 177 mass concentration', 'aerosol particle PCB 178 mass concentration', 'aerosol particle PCB 179 mass concentration', 'aerosol particle PCB 18 mass concentration', 'aerosol particle PCB 180 mass concentration', 'aerosol particle PCB 183 mass concentration', 'aerosol particle PCB 185 mass concentration', 'aerosol particle PCB 187 mass concentration', 'aerosol particle PCB 188 mass concentration', 'aerosol particle PCB 189 mass concentration', 'aerosol particle PCB 19 mass concentration', 'aerosol particle PCB 191 mass concentration', 'aerosol particle PCB 193 mass concentration', 'aerosol particle PCB 194 mass concentration', 'aerosol particle PCB 195 mass concentration', 'aerosol particle PCB 198 mass concentration', 'aerosol particle PCB 199 mass concentration', 'aerosol particle PCB 200 mass concentration', 'aerosol particle PCB 201 mass concentration', 'aerosol particle PCB 202 mass concentration', 'aerosol particle PCB 205 mass concentration', 'aerosol particle PCB 206 mass concentration', 'aerosol particle PCB 207 mass concentration', 'aerosol particle PCB 208 mass concentration', 'aerosol particle PCB 209 mass concentration', 'aerosol particle PCB 22 mass concentration', 'aerosol particle PCB 25 mass concentration', 'aerosol particle PCB 26 mass concentration', 'aerosol particle PCB 28 mass concentration', 'aerosol particle PCB 3 mass concentration', 'aerosol particle PCB 31 mass concentration', 'aerosol particle PCB 33 mass concentration', 'aerosol particle PCB 37 mass concentration', 'aerosol particle PCB 40 mass concentration', 'aerosol particle PCB 42 mass concentration', 'aerosol particle PCB 44 mass concentration', 'aerosol particle PCB 45 mass concentration', 'aerosol particle PCB 46 mass concentration', 'aerosol particle PCB 47 mass concentration', 'aerosol particle PCB 48 mass concentration', 'aerosol particle PCB 49 mass concentration', 'aerosol particle PCB 52 mass concentration', 'aerosol particle PCB 6 mass concentration', 'aerosol particle PCB 64 mass concentration', 'aerosol particle PCB 66 mass concentration', 'aerosol particle PCB 7 mass concentration', 'aerosol particle PCB 70 mass concentration', 'aerosol particle PCB 74 mass concentration', 'aerosol particle PCB 77 mass concentration', 'aerosol particle PCB 8 mass concentration', 'aerosol particle PCB 81 mass concentration', 'aerosol particle PCB 82 mass concentration', 'aerosol particle PCB 83 mass concentration', 'aerosol particle PCB 85 mass concentration', 'aerosol particle PCB 87 mass concentration', 'aerosol particle PCB 91 mass concentration', 'aerosol particle PCB 95 mass concentration', 'aerosol particle PCB 97 mass concentration', 'aerosol particle PCB 99 mass concentration', 'aerosol particle carbonate mass concentration', 'aerosol particle benzo(b)triphenylene mass concentration', 'aerosol particle dibenzo(a,e)pyrene mass concentration', 'aerosol particle dibenz(a,h)anthracene mass concentration', 'aerosol particle dibenzo(a,h)pyrene mass concentration', 'aerosol particle dibenzo(a,i)pyrene mass concentration', 'aerosol particle dibenzo(a,l)pyrene mass concentration', 'aerosol particle dibenzothiophene mass concentration', 'aerosol particle galactosan mass concentration', 'aerosol particle gamma-hexachlorocyclohexane mass concentration', 'aerosol particle glucose mass concentration', 'aerosol particle heptachlor mass concentration', 'aerosol particle heptachlor epoxide mass concentration', 'aerosol particle indene mass concentration', 'aerosol particle indeno(1,2,3-cd)perylene mass concentration', 'aerosol particle hexahydroxycyclohexane mass concentration', 'aerosol particle myo-inositol mass concentration', 'aerosol particle mannose mass concentration', 'aerosol particle methanesulfonic acid mass concentration', 'aerosol particle acer pollen number concentration', 'aerosol particle alnus pollen number concentration', 'aerosol particle amaranthaceae pollen number concentration', 'aerosol particle ambrosia pollen number concentration', 'aerosol particle artemisia pollen number concentration', 'aerosol particle betula pollen number concentration', 'aerosol particle broussonetia pollen number concentration', 'aerosol particle carpinus pollen number concentration', 'aerosol particle castanea pollen number concentration', 'aerosol particle casuarina pollen number concentration', 'aerosol particle corylus pollen number concentration', 'aerosol particle cryptomeria pollen number concentration', 'aerosol particle cupressaceae pollen number concentration', 'aerosol particle fagus pollen number concentration', 'aerosol particle fraxinus pollen number concentration', 'aerosol particle morus pollen number concentration', 'aerosol particle olea pollen number concentration', 'aerosol particle ostrya pollen number concentration', 'aerosol particle picea pollen number concentration', 'aerosol particle pinaceae pollen number concentration', 'aerosol particle plantago pollen number concentration', 'aerosol particle platanus pollen number concentration', 'aerosol particle poaceae pollen number concentration', 'aerosol particle populus pollen number concentration', 'aerosol particle quercus pollen number concentration', 'aerosol particle rumex pollen number concentration', 'aerosol particle salix pollen number concentration', 'aerosol particle total pollen number concentration', 'aerosol particle ulmus pollen number concentration', 'aerosol particle urticaceae pollen number concentration', 'aerosol particle 1,2,3,4-tetrachlorobenzene mass concentration', 'aerosol particle tetralin mass concentration', 'aerosol particle 1,2,3-trichlorobenzene mass concentration', 'aerosol particle 1,2,4,5-tetrachlorobenzene mass concentration', 'aerosol particle 1,2,4-trichlorobenzene mass concentration', 'aerosol particle 1,2-dichlorobenzene mass concentration', 'aerosol particle 1,3,5-trichlorobenzene mass concentration', 'aerosol particle 1,3-dichlorobenzene mass concentration', 'aerosol particle 1,4-dichlorobenzene mass concentration', 'aerosol particle 1-methylnaphthalene mass concentration', 'aerosol particle 1,2,3,4,6,7,8-heptachlorodibenzo-p-dioxin mass concentration', 'aerosol particle 1,2,3,4,6,7,8-heptachlorodibenzofuran mass concentration', 'aerosol particle 1,2,3,4,7,8,9-heptachlorodibenzofuran mass concentration', 'aerosol particle 1,2,3,4,7,8-hexachlorodibenzo-p-dioxin mass concentration', 'aerosol particle 1,2,3,4,7,8-hexachlorodibenzofuran mass concentration', 'aerosol particle 1,2,3,6,7,8-hexachlorodibenzo-p-dioxin mass concentration', 'aerosol particle 1,2,3,6,7,8-hexachlorodibenzofuran mass concentration', 'aerosol particle 1,2,3,7,8,9-hexachlorodibenzo-p-dioxin mass concentration', 'aerosol particle 1,2,3,7,8,9-hexachlorodibenzofuran mass concentration', 'aerosol particle 1,2,3,7,8-pentachlorodibenzo-p-dioxin mass concentration', 'aerosol particle 1,2,3,7,8-pentachlorodibenzofuran mass concentration', 'aerosol particle 2-chloronaphthalene mass concentration', 'aerosol particle 2-methylerythritol mass concentration', 'aerosol particle 2-methylnaphthalene mass concentration', 'aerosol particle 2-methylthreitol mass concentration', 'aerosol particle 2,3,4,6,7,8-hexachlorodibenzofuran mass concentration', 'aerosol particle 2,3,4,7,8-pentachlorodibenzofuran mass concentration', 'aerosol particle 2,3,7,8-tetrachlorodibenzo-p-dioxin mass concentration', 'aerosol particle 2,3,7,8-tetrachlorodibenzofuran mass concentration', 'aerosol particle 5-methylchrysene mass concentration', 'aerosol particle allyl 2,4,6-tribromophenyl ether mass concentration', 'aerosol particle 2-bromoallyl-2,4,6-tribromophenyl ether mass concentration', "aerosol particle 2,2',4,5,5'-pentabromobiphenyl mass concentration", "aerosol particle 2,2',4,4',6-pentabromodiphenyl ether mass concentration", "aerosol particle 2,3',4,4',6-pentabromodiphenyl ether mass concentration", "aerosol particle 3,3',4,4',5-pentabromodiphenyl ether mass concentration", "aerosol particle 2,2',3,4,4',5'-hexabromodiphenyl ether mass concentration", "aerosol particle 4,4'-dibromodiphenyl ether mass concentration", "aerosol particle 2,2',4,4',5,5'-hexabromodiphenyl ether mass concentration", "aerosol particle 2,2',4,4',5,6'-hexabromodiphenyl ether mass concentration", 'aerosol particle hexabromodiphenyl ether mass concentration', "aerosol particle 2,3,3',4,4',5-hexabromodiphenyl ether mass concentration", "aerosol particle 2,3,4,4',5,6-hexabromodiphenyl ether mass concentration", "aerosol particle 2,2',4-tribromodiphenyl ether mass concentration", "aerosol particle 2,2',3,4,4',5',6-heptabromodiphenyl ether mass concentration", "aerosol particle 2,2',3,4,4',6,6'-heptabromodiphenyl ether mass concentration", "aerosol particle 2,3,3',4,4',5,6-heptabromodiphenyl ether mass concentration", "aerosol particle 2,3,3',4,4',5',6-heptabromodiphenyl ether mass concentration", "aerosol particle 2,2',3,3',4,4',6,6'-octabromodiphenyl ether mass concentration", "aerosol particle 2,2',3,3',5,5',6,6'-octabromodiphenyl ether mass concentration", "aerosol particle 2,2',3,4,4',5,5',6-octabromodiphenyl ether mass concentration", "aerosol particle 2,3,3',4,4',5,5',6-octabromodiphenyl ether mass concentration", 'aerosol particle nonabromodiphenyl ether mass concentration', "aerosol particle 2,2',3,3',4,4',5,6,6'-nonabromodiphenyl ether mass concentration", 'aerosol particle decabromodiphenyl ether mass concentration', "aerosol particle 2,4,4'-tribromodiphenyl ether mass concentration", "aerosol particle 2,2',4,4'-tetrabromodiphenyl ether mass concentration", "aerosol particle 2,2',4,5'-tetrabromodiphenyl ether mass concentration", "aerosol particle 2,3',4,4'-tetrabromodiphenyl ether mass concentration", 'aerosol particle 2,4-dibromodiphenyl ether mass concentration', "aerosol particle 2,3',4',6-tetrabromodiphenyl ether mass concentration", "aerosol particle 2,2',3,4,4'-pentabromodiphenyl ether mass concentration", "aerosol particle 2,2',4,4',5-pentabromodiphenyl ether mass concentration", 'aerosol particle bis(2-ethylhexyl) tetrabromophthalate mass concentration', 'aerosol particle 2,3-dibromopropyl-2,4,6-tribromophenyl ether mass concentration', 'aerosol particle hexabromobenzene mass concentration', 'aerosol particle hexabromocyclododecane mass concentration', 'aerosol particle hexachlorobenzene mass concentration', 'aerosol particle hexachlorobutadiene mass concentration', 'aerosol particle octachlorodibenzo-p-dioxin mass concentration', 'aerosol particle octachlorodibenzofuran mass concentration', 'aerosol particle pentabromobenzyl acrylate mass concentration', 'aerosol particle pentabromoethylbenzene mass concentration', 'aerosol particle pentabromotoluene mass concentration', 'aerosol particle acenaphthene mass concentration', 'aerosol particle acenaphthylene mass concentration', 'aerosol particle anthanthrene mass concentration', 'aerosol particle anthracene mass concentration', 'aerosol particle benzanthracene mass concentration', 'aerosol particle benzo(a)pyrene mass concentration', 'aerosol particle benz(e)acephenanthrylene mass concentration', 'aerosol particle 1,2-dihydrobenz(j)aceanthrylene mass concentration', 'aerosol particle chrysene mass concentration', 'aerosol particle chrysene triphenylene mass concentration', 'aerosol particle coronene mass concentration', 'aerosol particle fluoranthene mass concentration', 'aerosol particle fluorene mass concentration', 'aerosol particle naphthalene mass concentration', 'aerosol particle perylene mass concentration', 'aerosol particle phenanthrene mass concentration', 'aerosol particle pyrene mass concentration', 'aerosol particle retene mass concentration', 'aerosol particle beta-hexachlorocyclohexane mass concentration', 'aerosol particle beta-endosulfan mass concentration', 'aerosol particle dieldrin mass concentration', 'aerosol particle endosulfan mass concentration', 'aerosol particle endrin mass concentration', 'aerosol particle methoxychlor mass concentration', 'aerosol particle mirex mass concentration', 'aerosol particle oxychlordane mass concentration', 'aerosol particle 2,3,5,6-tetrabromo-p-xylene mass concentration', 'aerosol particle p,p-DDD mass concentration', 'aerosol particle p,p-DDE mass concentration', 'aerosol particle p,p-DDT mass concentration', 'aerosol particle (E)-chlordane mass concentration', 'aerosol particle (E)-nonachlor mass concentration', 'aerosol particle formate mass concentration', 'aerosol particle oxalate mass concentration', 'aerosol particle acetate mass concentration', 'aerosol particle ammonium nitrate mass concentration', 'aerosol particle bromide mass concentration', 'aerosol particle fluoride mass concentration', 'aerosol particle iodide mass concentration', 'aerosol particle nitrite mass concentration', 'aerosol particle aldrin mass concentration', 'aerosol particle tetrabromo-o-chlorotoluene mass concentration', 'aerosol particle pentachloroanisole mass concentration', 'aerosol particle pentachlorobenzene mass concentration', 'aerosol particle alpha-hexachlorocyclohexane mass concentration', 'aerosol particle alpha-endosulfan mass concentration', 'aerosol particle (Z)-chlordane mass concentration', 'aerosol particle octachlorostyrene mass concentration', 'aerosol particle tetrachloroveratrole mass concentration', 'aerosol particle trichloroveratrole mass concentration', 'aerosol particle trifluralin mass concentration', 'aerosol particle beryllium mass concentration', 'aerosol particle bismuth mass concentration', 'aerosol particle antimony mass concentration', 'aerosol particle cerium mass concentration', 'aerosol particle cesium mass concentration', 'aerosol particle dysprosium mass concentration', 'aerosol particle erbium mass concentration', 'aerosol particle europium mass concentration', 'aerosol particle gadolinium mass concentration', 'aerosol particle gallium mass concentration', 'aerosol particle germanium mass concentration', 'aerosol particle hafnium mass concentration', 'aerosol particle holmium mass concentration', 'aerosol particle lanthanum mass concentration', 'aerosol particle lithium mass concentration', 'aerosol particle lutetium mass concentration', 'aerosol particle neodymium mass concentration', 'aerosol particle niobium mass concentration', 'aerosol particle praseodymium mass concentration', 'aerosol particle ruthenium mass concentration', 'aerosol particle samarium mass concentration', 'aerosol particle tantalum mass concentration', 'aerosol particle terbium mass concentration', 'aerosol particle thallium mass concentration', 'aerosol particle thorium mass concentration', 'aerosol particle thulium mass concentration', 'aerosol particle tin mass concentration', 'aerosol particle tungsten mass concentration', 'aerosol particle uranium mass concentration', 'aerosol particle ytterbium mass concentration', 'aerosol particle yttrium mass concentration', 'aerosol particle photomirex mass concentration', 'aerosol particle cellulose mass concentration', 'aerosol particle fructose mass concentration', 'aerosol particle sucrose mass concentration', 'aerosol particle trehalose mass concentration', 'aerosol particle arabitol mass concentration', 'aerosol particle erythritol mass concentration', 'aerosol particle o,p-DDD mass concentration', 'aerosol particle o,p-DDE mass concentration', 'aerosol particle o,p-DDT mass concentration', 'aerosol particle mineral dust mass concentration', 'aerosol particle phosphate mass concentration', 'aerosol particle mannitol mass concentration', 'aerosol particle mannosan mass concentration', 'aerosol particle PCB 130 and PCB 176 mass concentration', 'aerosol particle PCB 144 and PCB 135 mass concentration', 'aerosol particle PCB 16 and PCB 32 mass concentration', 'aerosol particle PCB 17 and PCB 18 mass concentration', 'aerosol particle PCB 172 and PCB 197 mass concentration', 'aerosol particle PCB 178 and PCB 129 mass concentration', 'aerosol particle PCB 196 and PCB 203 mass concentration', 'aerosol particle PCB 201 and PCB 157 mass concentration', 'aerosol particle PCB 24 and PCB 27 mass concentration', 'aerosol particle PCB 28 and PCB 31 mass concentration', 'aerosol particle PCB 4 and PCB 10 mass concentration', 'aerosol particle PCB 41 and PCB 71 mass concentration', 'aerosol particle PCB 56 and PCB 60 mass concentration', 'aerosol particle PCB 70 and PCB 60 mass concentration', 'aerosol particle PCB 70 and PCB 76 mass concentration', 'aerosol particle PCB 8 and PCB 5 mass concentration', 'aerosol particle PCB 84 and PCB 89 mass concentration', 'aerosol particle dibenzo(ac,ah)anthracenes mass concentration', 'aerosol particle PBDE 25 and PBDE 28 mass concentration', 'aerosol particle PBDE 85 and PBDE 126 mass concentration', 'aerosol particle benz(bj)fluoranthenes mass concentration', 'aerosol particle benz(bjk)fluoranthenes mass concentration', 'aerosol particle benz(bk)fluoranthenes mass concentration', 'aerosol particle benz(jk)fluoranthenes mass concentration', 'aerosol particle volatile mass concentration', 'aerosol particle non-volatile mass concentration', 'aerosol particle hydrogen ion mass concentration', 'aerosol particle positive ion mass concentration'], 'Aerosol Physical Properties': ['aerosol particle number concentration', 'cloud condensation nuclei number concentration', 'aerosol particle nano number concentration', 'aerosol particle fine-mode number concentration', 'aerosol particle number size distribution', 'cloud condensation nuclei number size distribution', 'aerosol particle fine-mode number size distribution', 'aerosol particle nano number size distribution', 'naturally negatively charged nano aerosol particle number size distribution', 'naturally positively charged nano aerosol particle number size distribution', 'aerosol particle volatile number size distribution', 'aerosol particle non-volatile number size distribution'], 'NO2': ['nitrogen dioxide amount fraction', 'nitrogen dioxide mass concentration', 'nitrogen dioxide number concentration']}

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

    all_ecv_dataset = []

    for dataset in all_datasets:
        title = dataset['md_identification']['title']
        md_distribution_information = dataset['md_distribution_information']

        dataset_urls = [
            {'url': entry['dataset_url'], 'type': entry['protocol']}
            for entry in md_distribution_information
            if entry['protocol'] in ['OPeNDAP', 'HTTP']
        ]
        
        print(dataset_urls)

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
        
        platform_id = code

        time_period = [dataset['ex_temporal_extent']['time_period_begin'], dataset['ex_temporal_extent']['time_period_end']]

        if mapped_ecvs:
                
            ecv_dataset = {'title': title, 'urls': dataset_urls, 'layers': None, 'ecv_variables': mapped_ecvs, 'time_period': time_period, 'platform_id': platform_id}
            all_ecv_dataset.append(ecv_dataset)

        else:
            pass 

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
                if 'standard_name' not in da.attrs:
                    continue
                if variables_set is not None:
                    std_name = da.attrs['standard_name']
                    ecv_names = MAPPING_ACTRIS_ECV.get(std_name, [])
                    if std_name not in STATIC_PARAMETERS and variables_set.isdisjoint(ecv_names):
                        continue
                varlist.append(varname)
            return ds[varlist].load()
    except Exception as e:
        raise RuntimeError(f'Reading the ACTRIS dataset failed: {dataset_id}') from e

if __name__ == "__main__":
    #print(get_list_platforms())
    #print(query_datasets_stations(['w2kl']))
    #print('Read dataset function')
    dataset_id = "https://thredds.nilu.no/thredds/dodsC/ebas_doi/SM/XF/DX/SMXF-DXYP.nc"
    print(read_dataset(dataset_id))
    #print(read_dataset(dataset_id, variables_list=['aerosol particle number size distribution']))
    #print('End read dataset function')
