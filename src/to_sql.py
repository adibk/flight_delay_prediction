import pandas as pd
import numpy as np
import sys
import os

import mysql.connector
from mysql.connector import Error

import clean
df = clean.import_data(data_path='../data')
df = clean.clean_data(df)
def connect_to_database(database, host='localhost', user='root', password='password', v=True):
    try:
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            if v:
                print("Connected to MySQL Server version", db_info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            if v:
                print("You're connected to database:", record)
            return connection, cursor

    except Error as e:
        if v: 
            print("Error while connecting to MySQL", e)

    return None

def close_connection(connection, cursor):
    if cursor:
        cursor.close()
    if connection and connection.is_connected():
        connection.close()
        print("MySQL connection is closed")

def show_databases(connection):
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print("Databases:")
        for database in databases:
            print(database[0])  
    except Error as e:
        print("Error executing SHOW DATABASES command", e)

def execute_query(connection, cursor, query, params=None):
    try:
        cursor.execute(query, params)
        if query.strip().lower().startswith('select') or query.strip().lower().startswith('show'):
            result = cursor.fetchall()
            return result
        else:
            connection.commit()
            return cursor.rowcount
    except Error as e:
        print("Error executing query:", e)
        return None

def put_res(results, query_type="SELECT"):
    if not results:
        print("No results found.")
        return

    if query_type.lower() in ["select", "show"]:
        for row in results:
            print(row)
    else:
        print(f"Affected rows: {results}")


def create_database(connection, db_name):
    cursor = None
    try:
        cursor = connection.cursor()
        query = f"SHOW DATABASES LIKE '{db_name}'"
        result = execute_query(connection, cursor, query)
        if result:
            print(f"Database '{db_name}' already exists.")
        else:
            query = f"CREATE DATABASE {db_name}"
            result = execute_query(connection, cursor, query)
            print(f"Database '{db_name}' created successfully.")
            show_databases(connection)
    finally:
        if cursor:
            cursor.close()
def map_dtype_to_mysql(dtype, smallint=False):
    if pd.api.types.is_integer_dtype(dtype):
        if smallint:
            return "SMALLINT"
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "VARCHAR(255)"
def create_table_from_dataframe(connection, table_name, dataframe, column_types=None, foreign_keys=None, smallint=False):
    cursor = None
    try:
        cursor = connection.cursor()
        columns = dataframe.columns
        types = dataframe.dtypes

        column_definitions = []
        for col in columns:
            if column_types and col in column_types:
                col_type = column_types[col]
            else:
                col_type = map_dtype_to_mysql(types[col],  smallint=smallint)
            column_definitions.append(f"{col} {col_type}")

        if foreign_keys:
            for col, ref in foreign_keys.items():
                column_definitions.append(f"FOREIGN KEY ({col}) REFERENCES {ref}")

        query = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"
        result = execute_query(connection, cursor, query)
        print(f"Table '{table_name}' created successfully.")
    finally:
        if cursor:
            cursor.close()


def insert_dataframe_to_table(connection, table_name, dataframe):
    cursor = None
    try:
        cursor = connection.cursor()
        for _, row in dataframe.iterrows():
            columns = ", ".join(row.index)
            values = ", ".join(["%s"] * len(row))
            # row_values = [None if pd.isna(value) else value for value in row] 
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(query, tuple(row))
            # cursor.execute(query, tuple(row_values))
        connection.commit()
        print(f"Data inserted successfully into '{table_name}' table.")
    except Error as e:
        print("Error inserting data:", e)
    finally:
        if cursor:
            cursor.close()


con, cur = connect_to_database('sys')


df_origin = df[[col for col in df.columns if col.startswith('ORIGIN_')]].rename(columns=lambda x: x.replace('ORIGIN_', '')).drop_duplicates()
df_dest = df[[col for col in df.columns if col.startswith('DEST_')]].rename(columns=lambda x: x.replace('DEST_', '')).drop_duplicates()
df_location = pd.concat([df_origin, df_dest], ignore_index=True).drop_duplicates()


df_state = df_location[['STATE_ABR', 'STATE_FIPS', 'STATE_NM', 'WAC']].drop_duplicates()
df_state.rename(columns={'STATE_ABR': 'abr', 'STATE_FIPS': 'FIPS', 'STATE_NM': 'name'}, inplace=True)
column_types = {
    'abr': 'VARCHAR(2) PRIMARY KEY',
    'name': 'VARCHAR(100)',
    'FIPS': 'SMALLINT',
    'WAC': 'SMALLINT'
}
table_name = "states"
create_table_from_dataframe(con, table_name, df_state, column_types)
insert_dataframe_to_table(con, table_name, df_state)


df_airport = df_location[['AIRPORT_ID', 'AIRPORT_SEQ_ID', 'CITY_MARKET_ID', 'CITY_NAME', 'STATE_ABR']]
df_airport.rename(columns={'AIRPORT_ID': 'airport_ID', 'AIRPORT_SEQ_ID': 'airport_seq_ID','CITY_MARKET_ID': 'city_market_ID', 'CITY_NAME': 'city', 'STATE_ABR': 'state_ID'}, inplace=True)
df_airport
column_types = {
    'airport_seq_ID': 'INT PRIMARY KEY',
    'city': 'VARCHAR(100)',
    'state_ID': 'VARCHAR(2)' 
}
foreign_keys = {
    'state_ID': 'states(abr)' 
}
table_name = "airport"
create_table_from_dataframe(con, table_name, df_airport, column_types, foreign_keys)
insert_dataframe_to_table(con, table_name, df_airport)


carrier = df[['AIRLINE_ID', 'CARRIER']].drop_duplicates()
carrier.rename(columns={'AIRLINE_ID': 'airline_ID', 'CARRIER': 'carrier'}, inplace=True)
column_types = {
    'airline_ID': 'INT PRIMARY KEY',
    'carrier': 'VARCHAR(2)' 
}
table_name = "airline"
create_table_from_dataframe(con, table_name, carrier, column_types)
insert_dataframe_to_table(con, table_name, carrier)


df = df.reset_index().rename(columns={'index': 'ID'})
df['ID'] = df.index + 1 

df_plane_infos = df[['ID', 'FL_DATE', 'AIRLINE_ID', 'TAIL_NUM', 'FL_NUM', 'ORIGIN_AIRPORT_SEQ_ID', 'DEST_AIRPORT_SEQ_ID', 'CRS_DEP_TIME', 'CRS_ARR_TIME', 'CRS_ELAPSED_TIME', 'DISTANCE', 'DISTANCE_GROUP']]
df_delay = df[['ID', 'DEP_TIME', 'DEP_DELAY', 'DEP_DELAY_GROUP', 'TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON', 'TAXI_IN', 'ARR_TIME', 'ARR_DELAY', 'ARR_DELAY_GROUP', 'CANCELLED', 'DIVERTED', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY']]
df_plane_infos.rename(columns={
    'FL_DATE': 'date',
    'AIRLINE_ID': 'airline_ID',
    'TAIL_NUM': 'tail_num',
    'FL_NUM': 'fl_num',
    'ORIGIN_AIRPORT_SEQ_ID': 'origin_airport_seq_ID',
    'DEST_AIRPORT_SEQ_ID': 'dest_airport_seq_ID',
    'CRS_DEP_TIME': 'dep_time',
    'CRS_ARR_TIME': 'arr_time',
    'CRS_ELAPSED_TIME': 'duration',
    'DISTANCE': 'distance',
    'DISTANCE_GROUP': 'distance_group'
    }, inplace=True)

df_delay.rename(columns={
    'FL_DATE': 'date',
    'AIRLINE_ID': 'airline_ID',
    'TAIL_NUM': 'tail_num',
    'FL_NUM': 'fl_num',
    'ORIGIN_AIRPORT_SEQ_ID': 'origin_airport_seq_ID',
    'DEST_AIRPORT_SEQ_ID': 'dest_airport_seq_ID',
    'CRS_DEP_TIME': 'dep_time',
    'CRS_ARR_TIME': 'arr_time',
    'CRS_ELAPSED_TIME': 'duration',
    'DISTANCE': 'distance',
    'DISTANCE_GROUP': 'distance_group'
    }, inplace=True)
create_database(con, 'flight_prediction')

df_plane_infos = df_plane_infos.where(pd.notnull(df_plane_infos), None)
column_types = {
    'ID': 'INT PRIMARY KEY',
    'tail_num': 'VARCHAR(20)',
    'dep_time': 'SMALLINT',
    'arr_time': 'SMALLINT',
    'duration': 'SMALLINT',
    'distance': 'SMALLINT',
    'distance_group': 'SMALLINT'
}
foreign_keys = {
    'dest_airport_seq_id': 'airport(airport_seq_ID)',
    'origin_airport_seq_id': 'airport(airport_seq_ID)',
    'airline_ID': 'airline(airline_ID)',
}
table_name = "flight"
create_table_from_dataframe(con, table_name, df_plane_infos, column_types, foreign_keys)
insert_dataframe_to_table(con, table_name, df_plane_infos)

df_delay = df_delay.replace({pd.NA: None, np.nan: None})
column_types = {
    'ID': 'INT PRIMARY KEY',
}
table_name = "delay"
create_table_from_dataframe(con, table_name, df_delay, column_types, smallint=True)
insert_dataframe_to_table(con, table_name, df_delay)

add_foreign_key_query = """
ALTER TABLE flight
ADD CONSTRAINT fk_delay_id
FOREIGN KEY (ID) REFERENCES delay(ID);
"""
execute_query(con, cur, add_foreign_key_query)









close_connection(con, cur)

# drop_table_query = "DROP TABLE IF EXISTS states"
# execute_query(con, cur, drop_table_query)