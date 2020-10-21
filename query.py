import tools

qry = {
    'stats_query_hour':
        """
            SELECT 
            t2.station_name as Zählstelle,
            t2.Status as Status, 
            t2.lanes as Spuren, 
            t2.street_type as Typ,
            printf("%.0f", avg({0})) as [Avg {1}],
            max({0}) as [Max. {1}],
            min(date) as [Erster Wert], 
            max(date) as [Letzter Wert],
            count(*) as [Anz Werte]
        FROM 
            traffic_fact as t1 
            inner join station_FACT as t2 on t2.id = t1.station_id
        where 
            {2}
        group by 
            t2.station_name,
            t2.Status,
            t2.street_type;
        """,
    
    'barchart_query_hour':
        """
        SELECT station_id as station_name, {0}, avg({1}) as {1} 
        FROM traffic_fact t1  
        where {2} group by {0}
        """,

    'barchart_query_day': 
        """
        SELECT station_id as station_name, {0}, avg({1}) as {1} 
        FROM v_traffic_per_day
        where {2} group by {0}
        """,
    
    'barchart_query_week': 
        """
        SELECT station_id as station_name, {0}, avg({1}) as {1} 
        FROM v_traffic_per_week
        where {2} group by {0}
        """,

    'map_query_hour': 
        """
        SELECT t2.station_name, t2.long, t2.lat, avg({0}) as {0} 
        FROM traffic_fact t1  
        inner join station_fact t2 on t2.id = t1.station_id 
        where lat > 0 {1} group by t2.station_name, t2.long, t2.lat
        """,
    'map_query_day': 
        """SELECT t2.station_name, t2.long, t2.lat, avg({0}) as {0} 
        FROM v_traffic_per_day t1  
        inner join station_fact t2 on t2.id = t1.station_id 
        where lat > 0 and {1} group by t2.station_name, t2.long, t2.lat
        """,

    'heatmap_query_hour':
        """
        SELECT t2.station_name, {0}, avg({1}) as {1} 
        FROM traffic_fact t1  
        inner join station_fact t2 on t2.id = t1.station_id 
        where {2} group by t2.station_name, {0} 
        order by t2.station_name
        """,

    'heatmap_query_day':
        """
        SELECT t2.station_name, {0}, avg({1}) * 24 as {1} 
        FROM traffic_fact t1  
        inner join station_fact t2 on t2.id = t1.station_id 
        where {2} 
        group by t2.station_name, {0} 
        order by t2.station_name
        """,

    'timeseries_query_day_direction': 
        """SELECT station_id as station_name, direction_id, station_id, date as time, {0} as {0} 
            FROM v_traffic_per_day t1  
            where {1}
            """,

    'timeseries_query_day': 
        """SELECT station_id as station_name, direction_id, station_id, date as time, sum({0}) as {0} 
            FROM v_traffic_per_day t1  
            where {1}
            group by date, station_id, direction_id, station_id
            """,

    'timeseries_query_hour_direction':
        """
        SELECT station_id as station_name, station_id, direction_id, date_time as time, sum({0}) as {0}
        FROM traffic_fact t1  
        where {1}
        group by station_id, wed_of_week, direction_id
        """,
    
    'timeseries_query_hour':
        """
        SELECT station_id as station_name, station_id, date_time as time, sum({0}) as {0}
        FROM traffic_fact t1  
        where {1}
        group by station_id, wed_of_week
        """,
    
    'timeseries_query_week_direction':
        """
        SELECT station_id as station_name, station_id, direction_id, wed_of_week as time, sum({0}) as {0}
        FROM v_traffic_per_week t1  
        where {1}
        group by station_id, wed_of_week, direction_id
        """,
    
    'timeseries_query_week':
        """
        SELECT station_id as station_name, station_id, wed_of_week as time, sum({0}) as {0}
        FROM traffic_fact t1  
        where {1}
        group by station_id, wed_of_week
        """,
    

    'group_list_query':
        "select distinct {0} as grp from traffic_fact where {1} order by {0}",

    'truncate_table': "delete from {}",

    'delete_rows_where': "delete from {} where {}",
    
    'dataset_query': 'SELECT * FROM info where id = {}',
    
    # 'id_code_query': 'select id, code from lookup_code where category_id = {} order by {}',
    #'code_title_query': "select code, label from lookup_code where category_id = {} order by {}",
    'stats_query_day': 
    """
        SELECT 
            t2.station_name as Zählstelle,
            t2.Status as Status, 
            t2.lanes as Spuren, 
            t2.street_type as Typ,
            avg({0}) as [Avg {1}],
            max({0}) as [Max. {1}],
            min(date) as [Erster Wert], 
            max(date) as [Letzter Wert],
            count(*) as [Anz Werte]
        FROM 
            v_traffic_per_day as t1 
            inner join station_FACT as t2 on t2.id = t1.station_id
        where 
            total is not null and {2}
        group by 
            t2.station_name,
            t2.Status,
            t2.street_type;
        """,
    'stats_query_day_bak': # printf only works with python 3.9
    """
        SELECT 
            t2.station_name as Zählstelle,
            t2.Status as Status, 
            t2.lanes as Spuren, 
            t2.street_type as Typ,
            printf("%.0f", avg({0})) as [Avg {1}],
            max({0}) as [Max. {1}],
            min(date) as [Erster Wert], 
            max(date) as [Letzter Wert],
            count(*) as [Anz Werte]
        FROM 
            v_traffic_per_day as t1 
            inner join station_FACT as t2 on t2.id = t1.station_id
        where 
            total is not null and {2}
        group by 
            t2.station_name,
            t2.Status,
            t2.street_type;
        """,

    'year_list_query': "SELECT cast(substr(date_from,1,4) as int) as year_from, cast(substr(date_to,1,4) as int) as year_to from info where id = {}",
    
    'direction_list_query': "SELECT id, label FROM direction where station_id = {}",
    
    'lanes_list_query': 
        "SELECT lanes FROM station_fact where id = {}",
    
    'miv_station_list': 
        "select id, station_name from station_fact where miv_flag = 1 order by station_name",
    
    'fuss_station_list': 
        "select id, station_name from station_fact where fuss_data_flag = 1 order by station_name",
    
    'velo_station_list': 
        "select id, station_name from station_fact where velo_data_flag = 1 order by station_name",
    
    'all_directions_list': 
        "select id, station_id, label from direction order by label",
}