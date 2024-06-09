import pandas as pd

def import_data(data_path='data', verbose=True):
    file_names = [f'2016_{str(n+1).zfill(2)}.csv' for n in range(12)]

    dfs = []
    for i in range(12):
        dtype_spec = None
        if i in [2, 4, 5, 8, 9, 10, 11]:
            dtype_spec = {48: 'str'}
        elif i == 3:
            dtype_spec = {
                0: 'str', 1: 'str', 3: 'str', 4: 'str', 10: 'str', 11: 'str', 13: 'str',
                19: 'str', 20: 'str', 21: 'str', 22: 'str', 30: 'str', 36: 'str', 41: 'str', 48: 'str'
            }
        df_part = pd.read_csv(f'{data_path}/{file_names[i]}', on_bad_lines='skip', dtype=dtype_spec)
        if verbose:
            print(f'File {i + 1}: {df_part.shape}')
        dfs.append(df_part)
    df = pd.concat(dfs, ignore_index=True)

    return df

def clean_data(df, verbose=True):
    columns_to_drop = [
        'Unnamed: 64',
        'UNIQUE_CARRIER',
        'YEAR',
        'QUARTER',
        'MONTH',
        'DAY_OF_MONTH',
        'DAY_OF_WEEK',
        'FL_DATE_year',
        'FL_DATE_quarter',
        'FL_DATE_month',
        'FL_DATE_day_of_month',
        'FL_DATE_day_of_week',
        'DEP_TIME_BLK',
        'ARR_TIME_BLK',
        'CANCELLATION_CODE',
        'FLIGHTS',
        'DEP_DELAY_NEW',
        'ARR_DELAY_NEW',
        'DEP_DEL15',
        'ARR_DEL15',
        'FIRST_DEP_TIME',
        'TOTAL_ADD_GTIME',
        'LONGEST_ADD_GTIME'
    ]
    existing_drop = [col for col in columns_to_drop if col in df.columns]
    if existing_drop:
        if verbose:
            print(f'Dropping {existing_drop}')
        df.drop(columns=existing_drop, inplace=True)

        
    if verbose:
        print(f'Dropping few problematic lines')
    df.drop(df[df['FL_NUM'] == '582700-1759'].index, inplace=True)
    df.drop(df[df['TAXI_OUT'] == '11.00.00'].index, inplace=True)
    df.drop(df[df['FL_DATE'] == '5059'].index, inplace=True)
    # df = df.loc[~df['FL_DATE'].str.startswith('5059')]
    df.drop(df[df['ORIGIN_STATE_NM'] == 'Illinois",1842"'].index, inplace=True)
    df['ORIGIN_WAC'] = df['ORIGIN_WAC'].replace('7.00', 7)

    columns_to_check = ['CRS_ARR_TIME', 'CANCELLED', 'DIVERTED', 'CRS_ELAPSED_TIME', 'DISTANCE', 'DISTANCE_GROUP']
    if verbose:
        print(f'Dropping few Nan in {columns_to_check}')
    df.dropna(subset=columns_to_check, inplace=True)


    if verbose:
        print(f'Converting FL_DATE to datetime')
    df.loc[:, 'FL_DATE'] = pd.to_datetime(df['FL_DATE'])


    if verbose:
        print(f'Converting columns to Int')
        
    df['FL_NUM'] = df['FL_NUM'].astype(int)

    df['ORIGIN_AIRPORT_ID'] = df['ORIGIN_AIRPORT_ID'].astype(int)
    df['ORIGIN_CITY_MARKET_ID'] = df['ORIGIN_CITY_MARKET_ID'].astype(int)
    df['ORIGIN_STATE_FIPS'] = df['ORIGIN_STATE_FIPS'].astype(int)
    df['ORIGIN_WAC'] = df['ORIGIN_WAC'].astype(int)

    df['DEST_AIRPORT_ID'] = df['DEST_AIRPORT_ID'].astype(int)
    df['DEST_AIRPORT_SEQ_ID'] = df['DEST_AIRPORT_SEQ_ID'].astype(int)
    df['DEST_CITY_MARKET_ID'] = df['DEST_CITY_MARKET_ID'].astype(int)
    df['DEST_STATE_FIPS'] = df['DEST_STATE_FIPS'].astype(int)
    df['DEST_WAC'] = df['DEST_WAC'].astype(int)


    df['CRS_DEP_TIME'] = df['CRS_DEP_TIME'].astype(int)
    df['DEP_TIME'] = df['DEP_TIME'].astype(float)
    df['DEP_TIME'] = df['DEP_TIME'].astype('Int64')
    df['DEP_DELAY'] = df['DEP_DELAY'].astype('Int64')
    df['DEP_DELAY_GROUP'] = df['DEP_DELAY_GROUP'].astype('Int64')
    df['TAXI_OUT'] = df['TAXI_OUT'].astype(float)
    df['TAXI_OUT'] = df['TAXI_OUT'].astype('Int64')
    df['WHEELS_OFF'] = df['WHEELS_OFF'].astype(float)
    df['WHEELS_OFF'] = df['WHEELS_OFF'].astype('Int64')
    df['WHEELS_ON'] = df['WHEELS_ON'].astype(float)
    df['WHEELS_ON'] = df['WHEELS_ON'].astype('Int64')
    df['TAXI_IN'] = df['TAXI_IN'].astype(float)
    df['TAXI_IN'] = df['TAXI_IN'].astype('Int64')
    df['CRS_ARR_TIME'] = df['CRS_ARR_TIME'].astype(float)
    df['CRS_ARR_TIME'] = df['CRS_ARR_TIME'].astype('Int64')
    df['ARR_TIME'] = df['ARR_TIME'].astype(float)
    df['ARR_TIME'] = df['ARR_TIME'].astype('Int64')
    df['ARR_DELAY'] = df['ARR_DELAY'].astype(float)
    df['ARR_DELAY'] = df['ARR_DELAY'].astype('Int64')
    df['ARR_DELAY_GROUP'] = df['ARR_DELAY_GROUP'].astype(float)
    df['ARR_DELAY_GROUP'] = df['ARR_DELAY_GROUP'].astype('Int64')
    df['CANCELLED'] = df['CANCELLED'].astype(int)
    df['DIVERTED'] = df['DIVERTED'].astype(int)
    df['CRS_ELAPSED_TIME'] = df['CRS_ELAPSED_TIME'].astype(int)
    df['ACTUAL_ELAPSED_TIME'] = df['ACTUAL_ELAPSED_TIME'].astype('Int64')
    df['AIR_TIME'] = df['AIR_TIME'].astype('Int64')
    df['AIR_TIME'] = df['AIR_TIME'].astype('Int64')
    df['DISTANCE'] = df['DISTANCE'].astype('Int64')
    df['DISTANCE_GROUP'] = df['DISTANCE_GROUP'].astype('Int64')


    df['LATE_AIRCRAFT_DELAY'] = df['LATE_AIRCRAFT_DELAY'].astype('Int64')
    df['CARRIER_DELAY'] = df['CARRIER_DELAY'].astype('Int64')
    df['WEATHER_DELAY'] = df['WEATHER_DELAY'].astype('Int64')
    df['NAS_DELAY'] = df['NAS_DELAY'].astype('Int64')
    df['SECURITY_DELAY'] = df['SECURITY_DELAY'].astype('Int64')
    
    return df

if __name__ == "__main__":
    df = import_data()
    df = clean_data(df)
    print(df)



 
