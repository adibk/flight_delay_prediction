import pandas as pd
import numpy as np

import mysql.connector
from mysql.connector import Error


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

def fetch_data_to_dataframe(connection, query):
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(res, columns=columns)
        return df
    except Error as e:
        print(f"Error fetching data: {e}")
        return None
    finally:
        if cursor:
            cursor.close()

def load_data(con):
    query = '''
        SELECT 
            ID,
            date,
            dep_time,
            arr_time,
            distance
        FROM 
            flight
        '''
    df_flight = fetch_data_to_dataframe(con, query)
    
    query = '''
        SELECT 
            ID,
            arr_delay
        FROM 
            delay
        '''
    df_delay = fetch_data_to_dataframe(con, query)
    
    df = pd.merge(df_delay, df_flight, on='ID', how='inner')
    
    return df


