import mysql.connector as mysql
import pandas as pd
import numpy as np
import streamlit as st

# from datetime import date
import sqlalchemy as sql
import datetime
import pymysql
import sqlite3

from query import qry
import config as cn
import db_config as dbcn
import tools

pymysql.install_as_MySQLdb()
# import MySQLdb

conn = ''

def execute_non_query(cmd: str, cn)-> bool:
    """
    Executes a stored procedure, without a return value. it can only be applied to 
    the local database
    """

    ok = False
    _cursor = cn.cursor()
    tools.log('execute_non_query: ' + cmd)
    try:
        _cursor.execute(cmd)
        conn.commit()
        ok = True
    except Exception as ex:
        tools.log(ex)

    tools.log(ok)
    return ok
    
    
def execute_query(query: str, cn) -> pd.DataFrame:
    """
    Executes a query and returns a dataframe with the results
    """

    ok= False
    result = None
    tools.log('execute_query: ' + query)
    try:
        result = pd.read_sql_query(sql=query, con=cn)
        ok = True
    except Exception as ex:
        tools.log(ex)    

    tools.log(ok)
    return result


def init():
    """
    Reads the connection string and sets the sql_engine attribute.
    """
    
    global conn
    try:
        conn = sqlite3.connect('./traffic.sqlite3')
    except Exception as ex:
        tools.log(ex)  

def get_connection(srv: str, usr: str, passw: str, db: str)-> object:
    try:
        return mysql.connect(
            host=srv,
            user=usr,
            passwd=passw,
            database=db,
        )
    except Exception as ex:
        st.error(ex)
        return None

def get_single_value(qry, conn, col) -> str:
    df = execute_query(qry, conn)
    return(df[col][0])

def get_distinct_values(column_name, table_name, dataset_id, criteria):
    """
    Returns a list of unique values from a defined code column.
    """

    criteria = (' AND ' if criteria > '' else '') + criteria
    query = f"SELECT {column_name} FROM {table_name} where dataset_id = {dataset_id} {criteria} group by {column_name} order by {column_name}"
    result = execute_query(query, conn)
    result = result[column_name].tolist()
    return result


def obsolet_get_id_code_dic(cat: int, order_col: str) -> dict:
    """
    Returns a dictionary for a given code category with lookupid and code for this category
    """

    query = qry['id_code_query'].format(cat, order_col)
    df = execute_query(query, conn)
    dic = dict(zip(df['id'].tolist(), df['code'].tolist()))
    return dic

@st.cache
def get_all_directions_dic():
    """
    Returns a list 
    """

    query = qry['all_directions_list']
    df = execute_query(query, conn)
    
    dic = dict(zip(df['id'].tolist(), df['label'].tolist()))
    return dic

# @st.cache
def get_code_title_dic(cat: int, order_col: str) -> dict:
    query = qry['code_title_query'].format(cat, order_col)
    df = execute_query(query, conn)
    dic = dict(zip(df['code'].tolist(), df['label'].tolist()))
    return dic

@st.cache
def get_station_dic(traffic_type: int):
    # sql = f"select id, `code` from station where {traffic_type + '_flag'} = 1 order by `code`"
    
    if traffic_type == 1:
        query = qry['miv_station_list']
    elif traffic_type == 2:
        query = qry['velo_station_list']
    elif traffic_type == 3:
        query = qry['fuss_station_list']
    df = execute_query(query, conn)
    return dict(zip(df['id'].tolist(), df['station_name'].tolist()))


def get_year_list(traffic_type: int) -> list:
    query = qry['year_list_query'].format(traffic_type)
    df = execute_query(query, conn)
    return list(range(df['year_from'][0], df['year_to'][0] + 1))


def get_direction_dic(station_id: int):
    """
    include first all for all directions, then all directions for the given station
    """

    dic1 = {"0": 'Alle Richtungen'}
    query = qry['direction_list_query'].format(station_id)
    df = execute_query(query, conn)
    dic2 = dict(zip(df['id'].tolist(), df['label'].tolist()))
    dic1.update(dic2)
    return dic1


def get_lane_list(station_id: int) -> list:
    query = qry['lanes_list_query'].format(station_id)
    df = execute_query(query, conn)
    lst = list(range(1, int(df['lanes'][0])))
    return lst


#def get_connect_string() -> str:
#    """
#    returns the connection string
#    """
#
#    return f'mysql+pymysql://{dbcn.DB_USER}:{dbcn.DB_PASS}@{dbcn.DB_HOST}:{dbcn.DB_PORT}/{dbcn.DATABASE}?charset=utf8'


def count_rows(base_qry: str, conn: mysql.connection):
    """
    Returns the number of results for a given query and connection
    """

    cmd = f"select count(*) as number from ({base_qry}) as t1"
    result = get_single_value(cmd, conn, 'number')
    return result