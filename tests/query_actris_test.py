import pandas as pd
from atmoaccess_data_access import query_actris

if __name__ == '__main__':
    platforms = query_actris.get_list_platforms()
    platforms_df = pd.DataFrame.from_records(platforms)
    print(platforms_df)

    variables = query_actris.get_list_variables()
    variables_df = pd.DataFrame.from_records(variables)
    print(variables_df)

    first_platform = platforms_df['short_name'].iloc[0]
    print(first_platform)
    # random_platform = platforms_df['short_name'].sample().iloc[0]
    datasets = query_actris.query_datasets_stations([first_platform])
    print(datasets)
