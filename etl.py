import streamlit as st
import sqlalchemy as sql
import pandas as pd
import numpy as np

import database as db
import query as all_qry
import tools
import config as cn
import os

"""
queries with etl commands used by operator module
""" 

# queries
qry = {'station_source_staging':
    """
    INSERT INTO station_staging (
        station_code,station_name,lat,long,location,class,type,lanes,purpose,type,street_type,property,date_start,status
	    )
        select
            zst_nr,zst_name,breitengr,laengengr,gemeinde,zst_klasse,art,anz_fahrst,zweck,typ,str_typ,eigentum,dat_inbetr,gueltig
        from 
            station_source;
    """,

    'miv_traffic_source_staging':
    """
        INSERT INTO traffic_staging (
        station_code,direction_name,lane,values_approved,values_edited,traffic_type,"total",MR,pw,lief,pw_lief,lw,sattelzug,lw_sattelzug,Bus,andere,
        month, year, weekday, hour_from, 
        date, date_time
        )
    select
        SiteCode,DirectionName,LaneCode,ValuesApproved,ValuesEdited,1,
        "total",mr,pw + "pw+",lief, lief + "lief+Aufl.","LW" + "LW+",sattelzug,lw + sattelzug, bus, andere, 
        Month, Year, Weekday + 1, HourFrom,
        printf('%s-%s-%s', substr(date,7,4), substr(date,4,2), substr(date,1,2) ),
        printf('%s-%s-%s %s:%s', substr(date,7,4), substr(date,4,2), substr(date,1,2), substr(timefrom,1,2), '30')
    from 
        miv_traffic_source;
    """,

    'slow_traffic_source_staging':
    """
        INSERT INTO traffic_staging (
        station_code,direction_name,lane,values_approved,values_edited,traffic_type,"total", 
        month, year, weekday, hour_from, 
        date, 
        date_time
        )
    select
        SiteCode,DirectionName,LaneCode,ValuesApproved,ValuesEdited, case when TrafficType = 'Velo' then 2 else 3 end,"total", 
        Month, Year, Weekday + 1, HourFrom,
        printf('%s-%s-%s', substr(date,7,4), substr(date,4,2), substr(date,1,2) ),
        printf('%s-%s-%s %s:%s', substr(date,7,4), substr(date,4,2), substr(date,1,2), substr(timefrom,1,2), '30')
    from 
        slow_traffic_source;
    """,

    'traffic_update_station_id':
    """
    update traffic_staging set station_id = 
    (select id from station_fact where station_code = traffic_staging.station_code)
    """,

    'traffic_update_direction_codes':
    """
    insert into direction(station_id,label)
    select 
        t1.station_id,t1.direction_name
    from 
        (select station_id, Direction_Name from traffic_staging
        group by station_id, Direction_Name) t1
        left join direction t2 on t2.station_id = t1.station_id and t2.label = t1.direction_name
    where t2.label is null
    """,

    'traffic_update_direction_id':
    """
    update traffic_staging set direction_id = 
    (select id from direction where station_id = traffic_staging.station_id and traffic_staging.direction_name = label)
    """,

    'station_staging_fact':
    """
    insert into station_fact (station_code,station_name,lat,long,location,class,type,lanes,purpose,TYP,street_type,property,date_start,status,url,miv_flag,slow_flag,fuss_data_flag,velo_data_flag)
    select 
        t1.station_code,t1.station_name,t1.lat,t1.long,t1.location,t1.class,t1.type,t1.lanes,t1.purpose,t1.TYP,t1.street_type,t1.property,t1.date_start,t1.status,t1.url,t1.miv_flag,t1.slow_flag,t1.fuss_data_flag,t1.velo_data_flag
    from
        station_staging t1
        left join station_fact t2 on t2.station_code = t1.station_code
    where 
        t2.station_code is null;

    """,
    'update_traffic_time_columns':
        """
        update traffic_staging set
        week = strftime('%W', date) + 1,
        wed_of_week = date(date, 'weekday 3'),  
        weekday_type = case when weekday < 6 then 1 else 2 end
        where week is null
    """,

    'traffic_staging_fact':
    """
    insert into traffic_fact (station_code,station_id,direction_id,values_approved,values_edited,
        lane,traffic_type,total,mr,pw,lief,pw_lief,lw,sattelzug,lw_sattelzug,bus,andere,date,date_time,hour_from,
        week,month,year,weekday,weekday_type,wed_of_week
    )
    select 
        station_code,station_id,direction_id,values_approved,values_edited,
        lane,traffic_type,total,mr,pw,lief,pw_lief,lw,sattelzug,lw_sattelzug,bus,andere,date,date_time,hour_from,
        week,month,year,weekday,weekday_type,wed_of_week
    from
        traffic_staging t1
    """,
    'update_info_table': """
     update info set 
        date_from = (select min(date) from miv_traffic),
	    date_to = (select max(date) from miv_traffic)
	 where 
        id = 1;
     """,

    'update_miv_info':
    """
    update info set date_from = (select min(date(date)) from traffic_fact where traffic_type = 1),
    date_to = (select max(date(date)) from traffic_fact where traffic_type = 1),
    number_of_stations = (select count(*) from station_fact where miv_flag = 1)
    where code = 'miv'
    """,

    'update_slow_info':
    """
    update info set date_from = (select min(date(date)) from traffic_fact where traffic_type > 1),
    date_to = (select max(date(date)) from traffic_fact where traffic_type > 1),
    number_of_stations = (select count(*) from station_fact where slow_flag = 1)
    where code = 'slow'
    """,

    'reset_station_flags':
    """
    update station_fact set miv_flag = 0, velo_data_flag= 0, fuss_data_flag=0; 
    """,
    
    'update_station_flags':
    """
    update station_fact set {} = 1 
        where id in (select station_id from traffic_fact where traffic_type = {});
    """,

    'last_miv_observation': "select max(date) as max_dat from traffic_fact",
     
    'append_miv_records_local2prod': "Select * from traffic_fact where date >= '{}'",

    'columns_for_table': 
    """
    select {} from information_schema.columns
        where table_schema = 'traffic' and table_name = '{}'
        order by table_name,ordinal_position
    """,

    'import_result_summary':
    """
    select 'miv' as type, year,count(*) as rows, count(distinct station_id) as stations from traffic_fact
    where traffic_type = 1
    group by year
    UNION
    select 'slow' as type, year,count(*) as rows, count(distinct station_id) as stations from traffic_fact
    where traffic_type > 1
    group by year
    order by type, year
    """,

    'insert_missing_records':
    """
    insert into traffic_fact (
        station_id,
        direction_id,
        values_edited,
        lane,
        traffic_type,
        date,
        date_time,
        hour_from,
        week,
        month,
        year,
        weekday,
        weekday_type
    )
    select 
        t1.station_id,
        t1.direction_id,
        t1.values_edited,
        t1.lane,
        t1.traffic_type,
        t1.date,
        t1.date_time,
        t1.hour_from,
        t1.week,
        t1.month,
        t1.year,
        t1.weekday,
        t1.weekday_type
    from 
    traffic_missing as t1
    left join traffic_fact t2 on t2.direction_id = t1.direction_id and t2.date_time = t1.date_time and t2.lane = t1.Lane
    where t2.id is null
    """,

    'min_max_dates':
        "select strftime('%m/%d/%Y',min(date)) as min, strftime('%m/%d/%Y',max(date)) as max from traffic_fact where year = {} and station_id = {}",
    
    'station_list_year': 
        "select distinct station_id, direction_id, lane, traffic_type from traffic_fact where year = {} and station_id > 0"
    }

# Functions

def save_df2table(table_name: str, df: pd.DataFrame, fields: list):
    """
    Saves selected columns of a pandas dataframe to a database table
    """

    st.info(f'Appending rows from dataframe to table {table_name}')
    ok = save_db_table(table_name, df, fields)
    if ok:
        st.info(f'Dataframe was appended to table {table_name}')
    else:
        st.error(f'Dataframe could not be appended to table {table_name}')
    return df, ok


def read_source_file(filename: str):
    """
    Reads a csv source file to a dataframe
    """

    df = pd.DataFrame
    ok = False
    st.info(f'Reading {filename}')
    try:
        df = pd.read_csv(filename, sep=';', encoding='UTF8')
        st.info(f'{len(df)} rows read to dataframe')
        ok = True
    except Exception as ex:
        st.error(f'There was an error loading the data file from opendata.bs: {ex}')
    return df, ok


def save_db_table(table_name: str, df: pd.DataFrame, fields: list):
    ok = False
    connect_string = 'sqlite:///traffic.sqlite3'
    try:
        sql_engine = sql.create_engine(connect_string, pool_recycle=3600)
        db_connection = sql_engine.connect()
    except Exception as ex:
        print(ex)

    try:
        if len(fields) > 0:
            df = df[fields]
            df.to_sql(table_name, db_connection, if_exists='append', chunksize=20000, index=False)
            tools.log(f'dataframe appended to {connect_string}')
            ok = True
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    finally:
        db_connection.close()
        return ok


def transfer_source2staging(key: str):
    """
    Copies data from the source to the staging table and updates some fields, returns status
    """
    ok = True    
    st.info('transferring data from source to staging')
    cmd = qry[key]
    ok = db.execute_non_query(cmd, db.conn)
    if ok:
        st.info('data transferred')
    else:
        st.info('An error occurred transferring the data from source to staging')
    return ok

def compact_db():
    st.info('Resetting tables...')
    cmd = all_qry.qry['truncate_table'].format('slow_traffic_source') 
    ok = db.execute_non_query(cmd, db.conn)
    if ok:
        cmd = all_qry.qry['truncate_table'].format('miv_traffic_source') 
        ok = db.execute_non_query(cmd, db.conn)
    if ok:
        cmd = all_qry.qry['truncate_table'].format('traffic_missing') 
        ok = db.execute_non_query(cmd, db.conn)
        st.info('Done...')
    st.info('Compacting database...')
    size_start = os.path.getsize(cn.DATABASE_FILE)
    if ok:
        cmd="VACUUM"
        ok = db.execute_non_query(cmd, db.conn)
    if ok:
        size_end = os.path.getsize(cn.DATABASE_FILE)
        st.info(f'database has been compacted from {size_start} to {size_end} bytes. ({(size_start-size_end) / size_start * 100} %')
    else:
        st.error('I tried but it did not work')

def import_traffic(year: int, traffic_type: int):
    """
    Replaces the table traffic_source with new data. Truncates first all data, then loads the all data from
    data.bs.ch in a dataframe and filters for rows with year > currentyear -2. this is to reduce the amount of data
    replaced in the target table miv.
    """

    def transfer_staging2fact():
        ok = True
        tools.log('Copy miv traffic records from staging to miv_traffic table')
        cmd = qry['traffic_staging_fact']

        ok = db.execute_non_query(cmd, db.conn)
        if ok:
            cmd = qry[update_info_query[traffic_type]]
            ok = db.execute_non_query(cmd, db.conn)
        if ok: 
            cmd = qry['reset_station_flags']
            ok = db.execute_non_query(cmd, db.conn)
        if ok:
            cmd = qry['update_station_flags'].format('miv_flag',1)
            ok = db.execute_non_query(cmd, db.conn)
            cmd = qry['update_station_flags'].format('velo_data_flag',2)
            ok = db.execute_non_query(cmd, db.conn)
            cmd = qry['update_station_flags'].format('fuss_data_flag',3)
            ok = db.execute_non_query(cmd, db.conn)
        if ok:
            st.info('Statement executed.')
        else:
            st.error('Statement failed.')
        return ok

        # cmd =  qry['last_miv_observation']
        # result = get_single_value(cmd, conn, 'max_dat')
        # st.info('Most recent observation in miv_traffic: '  + result)
    
    ### Main
    ok = True
    update_info_query = {1: 'update_miv_info',2: 'update_slow_info'}
    source_table = {1: 'miv_traffic_source', 2: 'slow_traffic_source'}
    source_file = {1: cn.source_miv_file_name.format(year), 2:cn.source_slow_file_name.format(year)}
    source_fields = {1: ['SiteCode', 'SiteName', 'DirectionName', 'LaneCode', 'LaneName', 'Date','TimeFrom','TimeTo',
                'ValuesApproved', 'ValuesEdited', 'TrafficType', 'Total', 'MR', 'PW', 'PW+', 'Lief', 'Lief+',
                'Lief+Aufl.', 'LW', 'LW+', 'Sattelzug', 'Bus', 'andere', 'Year', 'Month','Weekday','HourFrom'],
                2: ['SiteCode', 'SiteName', 'DirectionName', 'LaneCode', 'LaneName', 'Date','TimeFrom','TimeTo',
                'ValuesApproved', 'ValuesEdited', 'TrafficType', 'Total', 'Year', 'Month','Weekday','HourFrom']}
    source_staging_transfer_query = {1: 'miv_traffic_source_staging', 2: 'slow_traffic_source_staging'}
    traffic_type_criteria = {1: 'traffic_type = 1', 2: traffic_type > 1}
    row_count_start = db.count_rows("select * from traffic_fact", db.conn)

    # delete all records from the miv_traffic_source table
    if ok:
        cmd = all_qry.qry['truncate_table'].format(source_table[traffic_type]) 
        ok = db.execute_non_query(cmd, db.conn)
        if ok:
            st.info(f'Table {source_table[traffic_type]} was initialized.')
        else:
            st.error(f'Table {source_table[traffic_type]} could not be deleted.')
    if ok:
        df, ok = read_source_file(source_file[traffic_type])
    if ok:
        ok = save_db_table(source_table[traffic_type], df, source_fields[traffic_type])
    # delete all rows from the staging table 
    if ok:
        cmd = all_qry.qry['truncate_table'].format('traffic_staging') 
        ok = db.execute_non_query(cmd, db.conn)
        if ok:
            st.info(f'Table {"traffic_staging"} was initialized.')
        else:
            st.error(f'Table {"traffic_staging"} could not be deleted.')
    # copy the source data to the staging table, some fields are removed and counts are simplified, e.g. pw and pw with anhänger are summed
    # there is a new count for pw and lieferwagen and for lastwagen, lastwagen with anhänger and sattelschlepper so light and heavy traffic can be easily 
    # distinguished.
    if ok:
        ok = transfer_source2staging(source_staging_transfer_query[traffic_type])
    # get the station_id from the station table 
    if ok:
        cmd =  qry['traffic_update_station_id']
        ok = db.execute_non_query(cmd, db.conn)
    # append new direction names to the lookup table
    if ok:
        cmd =  qry['traffic_update_direction_codes']
        ok = db.execute_non_query(cmd, db.conn)
    # update direction id field in traffic_staging table
    if ok:
        cmd =  qry['traffic_update_direction_id']
        ok = db.execute_non_query(cmd, db.conn)
    # update time fields
    if ok:
        cmd =  qry['update_traffic_time_columns']
        ok = db.execute_non_query(cmd, db.conn)
    ok = True
    if ok:
        cmd = all_qry.qry['delete_rows_where'].format('traffic_fact', f'{traffic_type_criteria[traffic_type]} and year = {year}') 
        st.write(cmd)
        ok = db.execute_non_query(cmd, db.conn)
        if ok:
            st.info(f'Table {"traffic_fact"} was initialized for year and traffic type.')
        else:
            st.error(f'Table {"traffic_staging"} could not be initialized for year and traffic type.')
    if ok:
        ok = transfer_staging2fact()
    if ok:
        row_count_end = db.count_rows("select * from traffic_fact", db.conn)
        st.info(f'{row_count_end -  row_count_start} rows where successfully imported')
        df = db.execute_query(qry['import_result_summary'], db.conn)
        st.write("Summary")
        st.table(df)
    else:
        st.error("The import could not be completed, check log above for error messages")

def stations_import():
    """
    Imports the traffic site and updates the site table
    """

    def load_data_local():
        ok = True    
        st.info('transferring data from staging to fact')
        cmd = qry['station_staging_fact']
        ok = db.execute_non_query(cmd, db.conn)
        if ok:
            st.info('data transferred')
        else:
            st.info('An error occurred transferring the data from staging to fact')
        return ok
    
    # Main
    ok = True
    if ok:
        cmd = all_qry.qry['truncate_table'].format('station_source') 
        ok = db.execute_non_query(cmd, db.conn)
    if ok:
        df, ok = read_source_file(cn.SOURCE_STATION_FILE_NAME)
    if ok:
        fields = ['ZST_NR',	'ZST_NAME', 'Geo Point', 'BREITENGR', 'LAENGENGR', 'GEMEINDE', 'ZST_KLASSE', 'KOMBINIERT',	
            'ART', 'ANZ_ARME', 'ANZ_FAHRST', 'ZWECK', 'TYP', 'STR_TYP', 'EIGENTUM', 'DAT_INBETR', 'GUELTIG']
        ok = save_df2table('station_source', df, fields)
    ok= False
    if ok:
        cmd = all_qry.qry['truncate_table'].format('station_staging') 
        ok = db.execute_non_query(cmd, db.conn)
    if ok:
        ok = transfer_source2staging('station_source_staging')
    if ok:
        pass # not yet required: ok = update_columns()
    if ok:
        ok = load_data_local()

    ok = True
    tools.log('miv_update start')
    
def insert_all_missing_rows() -> bool:
    year_from = 2015 
    year_to= 2020
    ok = True
    for y in range(year_from, year_to + 1):
        insert_missing_rows(y) 
    
    return ok

def insert_missing_rows(year: int) -> bool:
    """
    Inserts an empty value row for all missing intervals. This allows to show missing values as gaps in 
    time series if plotted.
    """

    cmd = all_qry.qry['delete_rows_where'].format('traffic_fact', "values_edited=-99") 
    ok = db.execute_non_query(cmd, db.conn)
    if ok: 
        cmd = all_qry.qry['truncate_table'].format('traffic_missing') 
        ok = db.execute_non_query(cmd, db.conn)

    fields = ["direction_id","station_id", "values_edited","lane",
        "traffic_type","date","date_time","hour_from","week","month","year","weekday","weekday_type",
    ]
    cmd = qry['station_list_year'].format(year)
    directions = db.execute_query(cmd, db.conn)
    for row in directions.itertuples ():
        tools.log(f'{row.direction_id}, {row.lane}')
        # make sure that empty rows are only inserted with the min/max values of existing data.
        cmd = qry['min_max_dates'].format(year, row.station_id)
        df = db.execute_query(cmd, db.conn)
        start_date = df['min'][0]
        end_date = df['max'][0]
        idx_intervals = pd.date_range(start=f'{start_date}', end=f'{end_date}', freq='H', tz='Europe/Zurich') 
        df = pd.DataFrame(data = idx_intervals, columns = ['date_time'])   
        
        df['year'] = year
        df['station_id'] = row.station_id
        df['direction_id'] = row.direction_id
        df['lane'] = row.lane
        df['traffic_type'] = row.traffic_type
        df['date'] =  df['date_time'].dt.strftime('%Y-%m-%d')
        df['month'] = np.int64(df['date_time'].dt.strftime('%m'))
        df['week'] = np.int64(df['date_time'].dt.strftime('%W'))
        df['weekday'] = np.int64(df['date_time'].dt.strftime('%u'))
        df.loc[df['weekday'] < 6, 'weekday_type'] = 1
        df.loc[df['weekday'] > 5, 'weekday_type'] = 2
        df['hour_from'] = np.int64(df['date_time'].dt.strftime('%H'))
        df['values_edited'] = -99
        df['date_time'] = df['date_time'].dt.strftime('%Y-%m-%d %H:30') 
        if not ok:
            break
        else:
            ok = save_df2table('traffic_missing', df, fields)
        
    if ok:
        cmd = qry['insert_missing_records']
        ok = db.execute_non_query(cmd, db.conn)
        if ok:
            st.write('Added records intervals with no measured values')
            st.write('Finished')

    return ok