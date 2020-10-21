"""
This module contains the traffic class, which holds most of the functionality of the app
"""

__author__ = "lcalmbach@gmail.com"
__version__ = '0.4.0'

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import pydeck as pdk
from datetime import date
import datetime
import collections
# from datetime import date, timedelta, datetime

import config as cn
import tools
import database as db
from query import qry
import tools
import info


class Traffic:
    """
    Includes all functions to display application info, read data, renders the about text.
    """

    def __init__(self, result_type):
        db.init()

        self.__result_type = result_type
        self.marker_groupby = ''
        self.plot_groupby = ''
        self.marker_groupby = 'Year'
        self.define_axis_length = False
        self.plot_width = cn.def_plot_width
        self.plot_height = cn.def_plot_height
        self.define_axis_limits = False
        self.xax_min = 0
        self.xax_max = 0
        self.yax_min = 0
        self.yax_max = 0
        self.marker_average_method = ''
        self.stat_line_method = ''
        self.time_aggregation_interval = 'day'
        
        self.chart_default_fields = []
        self.moving_average_days = 0
        self.traffic_type = 1                  # slow traffic or motorized traffic
        self.has_time_aggregation_option = True
        self.has_parameter_option = True
        self.has_marker_group_by_option = True
        self.default_marker_index=3
        self.group_marker_legend = 'Gruppiere Balken nach'
        self.y_max_legend = 'Y-Achse Maximum'
        self.y_min_legend = 'Y-Achse Minimum'
        self.time_filter_type = 'keine'
        self.group_marker_options = cn.default_group_by_dic
        self.group_plot_options = cn.default_group_by_dic
        self.time_aggregation_options =  cn.default_time_aggregation_dic
        self.direction_dic = {}
        self.all_directions_dic = db.get_all_directions_dic()
        self.month_dic = cn.month_long_dic
        self.week_dic = tools.get_week_dic()
        self.sort_dic = {'weekday': list(cn.weekday_short_dic.values()),
                         'weekday_type': list(cn.weekday_type_dic.values())[1:2],
                         'month': list(cn.month_short_dic.values()),
                         'year': list(range(2015, 2030)),
                         'hour_from': list(cn.time_dic.values()),
                         'station_id': list(self.station_dic.values()),
                         'direction_id': [],
                         'week': list(range(1, 52))
                         }
        # filters
        self.station_filter = ''
        self.lane_filter = ''
        self.direction_filter = ''
        self.filter_by_date_flag = False
        self.filter_by_time_flag = False
        _yesterday = date.today() - datetime.timedelta(days=1)
        self.date_filter = [_yesterday, _yesterday]
        self.week_filter = 0
        self.time_filter = []
        self.month_filter = 0
        # todo: make last year default: currently if set this way, the slider cannot be changed afterwards? ask stremlit!
        self.year_filter = [self.year_list[-1], self.year_list[-1] -1] # current year is default for year restriction
        self.weekday_filter = 0
        self.weekday_type_filter = 0

    @property
    def traffic_type(self):
        return self.__traffic_type

    @traffic_type.setter
    def traffic_type(self, tt: int):
        self.__traffic_type = tt
        self.ypar = 'Total'
        self.station_dic = db.get_station_dic(self.traffic_type)
        self.parameter_dic = cn.parameter_dic[self.traffic_type]
        self.year_list = db.get_year_list(tt)

    @property
    def result_type(self):
        return self.__result_type

    @result_type.setter
    def result_type(self, rt: str):
        self.__result_type = rt

        
        if rt == 'Heatmap':
            # self.has_time_aggregation_option = False
            self.has_parameter_option = False
            self.group_marker_legend = 'Gruppiere Spalten nach'
            self.y_max_legend = 'Farb-Skala Maximum'
            self.y_min_legend = 'Farb-Skala Minimum'
        elif rt in ['Karte-Zählstelle','Karte-Verkehr']:
            self.marker_groupby = 'none'
            self.has_marker_group_by_option = False

        if rt == "Zeitreihe":
            self.group_marker_legend = 'Gruppiere Symbole nach'
            self.group_marker_options = cn.ts_marker_group_by_dic
            self.group_plot_options = cn.ts_plot_group_by_dic
            self.default_marker_index=0
            self.time_aggregation_options =  cn.timeseries_time_aggregation_dic
        elif rt == 'Statistik':
            self.has_marker_group_by_option = False

    def show_help_icon(self):
        """
        Renders a help icon linking to the <read the docs> user manual
        """

        st.sidebar.markdown(
            '<a href = "{}" target = "_blank"><img border="0" alt="Help" src="{}"></a>'.format(cn.USER_MANUAL_LINK,
                                                                                               cn.HELP_ICON),
            unsafe_allow_html=True)

    def show_dataset_info(self):
        """
        Displays general info on the application, this is the first view shown to the user. 
        Info texts for the 3 traffic types are kept in the info.py file.
        """

        st.image(cn.INFO_IMAGE_FILE[self.traffic_type])
        _query = qry['dataset_query'].format(self.traffic_type)
        _df = db.execute_query(_query, db.conn)
        _df['date_from'] =  pd.to_datetime(_df['date_from'], format='%Y-%m-%d')
        _df['date_to'] =  pd.to_datetime(_df['date_to'], format='%Y-%m-%d')
        _min_date = _df['date_from'][0].strftime('%d.%m.%Y')
        _max_date = _df['date_to'][0].strftime('%d.%m.%Y')
        _title = _df['title'][0]
        _num = _df['number_of_stations'][0]
        _fahrzeug_typen = tools.get_cs_item_list(self.parameter_dic.values()) if self.traffic_type == 1 else ''
        _text = info.text[self.traffic_type].format(_title, _num, min(self.year_list),
                               _fahrzeug_typen, _min_date, _max_date)
        st.markdown(_text)

    def show_about_box(self):
        """
        Renders the about text in the sidebar.
        """

        st.sidebar.subheader("About")
        st.sidebar.info(cn.ABOUT_TEXT)

    def show_sidebar_controls(self):
        """
        Renders all sidebar controls
        - groupby section
        - parameters section
        - filter section
        """

        def show_group_by_controls():
            """
            Group by controls for plots and markers
            """

            st.sidebar.markdown('---')

            if self.has_marker_group_by_option:
                _list = list(self.group_marker_options.keys())
                self.marker_groupby = st.sidebar.selectbox(self.group_marker_legend, index=self.default_marker_index,
                                                            options=_list,
                                                            format_func=lambda x: self.group_marker_options[x])

            _list = list(self.group_plot_options.keys())
            _label = 'Tabellen' if self.result_type == 'Statistik' else 'Grafiken'
            
            self.plot_groupby = st.sidebar.selectbox('Gruppiere {} nach'.format(_label), _list,
                                                     format_func=lambda x: self.group_plot_options[x])




            # if self.result_type == 'Zeitreihe':
            #    self.marker_groupby = 'station_id'

        def show_parameter_controls():
            """
            Allows the user to select a parameter
            """

            st.sidebar.markdown('---')

            if self.plot_groupby != 'Fahrzeugtyp':
                self.ypar = st.sidebar.selectbox('Parameter für Analyse', list(self.parameter_dic.keys()),
                                                 format_func=lambda x: self.parameter_dic[x])

            # self.__showdata_table = st.sidebar.checkbox('Show data table', value=False, key=None)

            if self.has_time_aggregation_option:
                self.time_aggregation_interval = st.sidebar.selectbox('Zeitliche Aggregation der Messungen',
                                                                  list(self.time_aggregation_options.keys()),
                                                                  format_func=lambda x: self.time_aggregation_options[x])

        def show_filter_controls():
            """
            Renders the filter widgets in the sidebar. Allows the user to filter the results
            """

            st.sidebar.markdown('---')
            st.sidebar.markdown('#### Filter')
            # only provide the all option for stations, if the plots are grouped by station
            if self.plot_groupby == 'station_id' or self.result_type == 'Statistik' or self.result_type in ('Karte-Zählstelle',
                                                                                    'Karte-Verkehr','Heatmap', 'Balkendiagramm'):
                dic = {0: 'Alle'}
                dic.update(self.station_dic)
                self.station_dic = dic
            else:
                dic = self.station_dic

    
            self.station_filter = st.sidebar.selectbox(label='Zählstelle', index=0,
                                                    options=list(dic.keys()),
                                                    format_func=lambda x: dic[x])
            if self.station_filter != 0:
                self.direction_dic = db.get_direction_dic(self.station_filter)
                self.direction_filter = st.sidebar.selectbox(label='Richtung', index=0,
                                                            options=list(self.direction_dic.keys()),
                                                            format_func=lambda x: self.direction_dic[x])

            self.filter_by_date_flag = st.sidebar.checkbox("Nach Datum Filtern",value = self.filter_by_date_flag)
            if self.filter_by_date_flag:
                self.date_filter[0] = st.sidebar.date_input('Von Datum')
                self.date_filter[1] = st.sidebar.date_input('Bis Datum')
            else:
                self.weekday_filter = st.sidebar.selectbox(label='Wochentag', index=0,
                                                        options=list(cn.weekday_short_dic.keys()),
                                                        format_func=lambda x: cn.weekday_short_dic[x])
                self.weekday_type_filter = st.sidebar.selectbox(label='Werktage/Wochenende', index=0,
                                                        options=list(cn.weekday_type_dic.keys()),
                                                        format_func=lambda x: cn.weekday_type_dic[x])
                self.week_filter = st.sidebar.selectbox(label='Auswahl Woche', index=0,
                                                        options=list(self.week_dic.keys()),
                                                        format_func=lambda x: self.week_dic[x])
                self.month_filter = st.sidebar.selectbox(label='Auswahl Monat', index=0,
                                                        options=list(self.month_dic.keys()),
                                                        format_func=lambda x: self.month_dic[x])
                self.year_filter = st.sidebar.slider('Auswahl Jahr(e)',
                                                        min_value=self.year_list[0], max_value=self.year_list[-1],
                                                        value=self.year_filter)                      
            
            self.time_filter = st.sidebar.select_slider('Auswahl Zeit (z.B. 2 für 02-03h)',
                                                        options=list(cn.time_dic.keys()),
                                                        format_func=lambda x: cn.time_dic[x],
                                                        value = [0,23])
                                                                    

        def show_plot_setting_controls():
            """
            Allows the user to change plot settings such as axis start/end
            """
            
            st.sidebar.markdown('---')
            st.sidebar.markdown('#### Grafik Einstellungen')

            if self.result_type == 'Zeitreihe':
                self.moving_average_days = st.sidebar.number_input(label='Gleitender Durchschnitt, Fenster in Tagen', value=0)

            # only provide the all option for stations, if the plots are grouped by station
            if self.result_type in ('X-Y-Plot'):
                self.xax_min = st.sidebar.number_input(label='X-Achse Minimum', value=0.0)
                self.xax_max = st.sidebar.number_input(label='X-Achse Maximum', value=0.0)
            self.yax_min = st.sidebar.number_input(label=self.y_min_legend, value=0.0)
            self.yax_max = st.sidebar.number_input(label=self.y_max_legend, value=0.0)

            self.plot_width = st.sidebar.number_input(label='X-Achse Länge (Pixel)', value=cn.def_plot_width)
            self.plot_height = st.sidebar.number_input(label='Y-Achse Länge (Pixel)', value=cn.def_plot_height)

        show_group_by_controls()
        show_parameter_controls()
        show_filter_controls()
        if self.result_type not in ('Statistik', 'Karte-Zählstelle','Karte-Verkehr'):
            show_plot_setting_controls()

    def show_results(self):
        """
        Renders the result (table or plot) in the main window
        """

        def get_number_expression():
            if self.traffic_type == cn.TRAFFIC_TYPE_MIV:
                return self.parameter_dic[self.ypar]
            elif self.traffic_type == 2:
                return 'Velos'
            else:
                return 'Fussgänger'

        def get_info():
            info = self.time_aggregation_options[self.time_aggregation_interval]
            info += ('' if self.station_filter == '0' else
                     f', Zählstelle: {self.station_dic[self.station_filter]}')
            info += (', Alle Richtungen' if self.direction_filter in ('0', '') else
                     ', Richtung: {}'.format(self.direction_dic[self.direction_filter]))
            # info += (', Alle Spuren' if self.lane_filter in ('Alle', '') else ', Spur {}'.format(self.lane_filter))
            if self.filter_by_date_flag:
                if self.date_filter[0] == self.date_filter[1]:
                    info += ', Datum: {}'.format(self.date_filter[0].strftime("%d.%m.%Y"))
                else:
                    info += f', Datum: {self.date_filter[0].strftime("%d.%m.%Y")} - {self.date_filter[1].strftime("%d.%m.%Y")}'
            if has_week_filter():
                info += f', Kalenderwoche: {self.week_filter}'
            elif has_month_filter():
                info += f', Monat: {self.month_dic[self.month_filter]}'
            elif has_year_filter():
                if self.year_filter[0] == self.year_filter[1]:
                    info += f', Jahr: {self.year_filter[0]}'
                else:
                    info += f', Jahre: {self.year_filter[0]} - {self.year_filter[1]}'
            if has_time_filter():
                info += f', Zeit: {self.time_filter[0]}h - {self.time_filter[1] +1}h'
            if has_weekday_filter():
                info += f', Wochentag: {cn.weekday_long_dic[self.weekday_filter]}'
            if has_weekday_type_filter():
                info += f', {cn.weekday_type_dic[self.weekday_type_filter]}'
            return info


        def substitute_codes(df):
            if 'station_name' in df.columns:
                df['station_name'].replace(self.station_dic, inplace=True)

            if self.marker_groupby == 'weekday_from':
                df['weekday_from'].replace(cn.weekday_short_dic, inplace=True)
            elif self.marker_groupby == 'month_from':
                df['month_from'].replace(cn.month_short_dic, inplace=True)
            elif self.marker_groupby == 'hour_from':
                df['hour_from'].replace(cn.time_dic, inplace=True)
            elif self.marker_groupby == 'station_id':
                df['station_id'].replace(self.station_dic, inplace=True)
            elif self.marker_groupby == 'weekday_type':
                df['weekday_type'].replace(cn.weekday_type_dic, inplace=True)
            elif self.marker_groupby == 'weekday':
                df['weekday'].replace(cn.weekday_long_dic, inplace=True)
            if 'direction_id' in df.columns:
                df['direction_id'].replace(self.all_directions_dic, inplace=True)
            return df

        def fill_stat_df(base_query: str, criteria: str) -> pd.DataFrame:
            # query = base_query.format(self.ypar, self.parameter_dic[self.traffic_type][self.ypar], '{0}')
            _query = base_query.format(criteria)
            df = db.execute_query(_query, db.conn)
            return df

        def fill_single_plot_df(base_query: str, par: str, criteria: str) -> pd.DataFrame:
            _query = base_query.format(self.marker_groupby, par, criteria)
            try:
                _df = db.execute_query(_query, db.conn)
                _df = substitute_codes(_df)
                return _df
            except:
                return pd.DataFrame()

        def fill_heatmap_df(base_query: str, par: str, criteria: str) -> pd.DataFrame:
            query = base_query.format(self.marker_groupby, par, criteria)
            df = db.execute_query(query, db.conn)
            df = substitute_codes(df)
            return df

        def fill_single_map_df(base_query: str, par: str, criteria: str) -> pd.DataFrame:
            _query = base_query.format(par, criteria)
            try:
                _df = db.execute_query(_query, db.conn)
                _df = substitute_codes(_df)
                return _df
            except:
                return pd.DataFrame()

        def show_single_result():
            _title = self.station_dic[self.station_filter]
            st.markdown(f'## Anzahl {get_number_expression()} an Zählstelle')
            st.markdown(get_info())
            if self.result_type == 'Statistik':
                key = f'stats_query_{self.time_aggregation_interval}'
                query = qry[key].format(self.ypar,cn.parameter_dic[self.traffic_type][self.ypar], '{}')   
                df = fill_stat_df(query, get_criteria())
                if len(df)>0:
                    st.dataframe(df)
                    st.markdown(tools.get_table_download_link(df), unsafe_allow_html=True)
                else:
                    st.warning('Keine Daten gefunden, bitte ändern sie die Filter-Einstellungen')
            elif self.result_type == 'Balkendiagramm':
                _key = 'barchart_query_' + self.time_aggregation_interval
                _query = qry[_key]
                _par = self.ypar if self.traffic_type == cn.TRAFFIC_TYPE_MIV else 'total'
                _df = fill_single_plot_df(_query, _par, get_criteria())
                if len(_df) == 0:
                    st.warning('Keine Daten gefunden, bitte ändern sie die Filter-Einstellungen')
                else:
                    if self.traffic_type == cn.TRAFFIC_TYPE_MIV:
                        _par = self.ypar
                        _y_ax_label = self.parameter_dic[self.ypar]
                    else:
                        _par = self.parameter_dic[self.ypar]
                        _y_ax_label = _par
                        _df.rename(columns={'total': _par}, inplace=True)

                    show_barchart(_title, _df, _par, _y_ax_label)

            elif self.result_type == 'Zeitreihe':
                _key = 'timeseries_query_' + self.time_aggregation_interval 
                _key += ('' if self.marker_groupby == 'none' else '_direction')
                _query = qry[_key]
                _par = self.ypar if self.traffic_type == cn.TRAFFIC_TYPE_MIV else 'total'
                _df = fill_single_map_df(_query, _par, get_criteria())
                if len(_df) == 0:
                    st.warning('Keine Daten gefunden, bitte überprüfen sie die Filter-Einstellungen')
                else:
                    if self.traffic_type == cn.TRAFFIC_TYPE_MIV:
                        _par = self.ypar
                        _y_ax_label = self.parameter_dic[self.ypar]
                    else:
                        _par = self.parameter_dic[self.ypar]
                        _y_ax_label = _par
                    
                    _df.rename(columns={'total': _par, 'direction_id': 'Richtung'}, inplace=True)
                    show_time_series(_title, _df, _par, _y_ax_label)
            elif self.result_type == 'Karte-Zählstelle':
                key = 'map_query_' + self.time_aggregation_interval
                query = qry[key]
                par = self.ypar if self.traffic_type == cn.TRAFFIC_TYPE_MIV else 'total'
                df = fill_single_map_df(query, par, get_criteria())
                if len(df) == 0:
                    st.warning('Keine Daten gefunden, bitte ändern sie die Filter-Einstellungen')
                else:
                    show_map(_title, df, par)
            elif self.result_type == 'Karte-Verkehr':
                key = 'map_query_' + self.time_aggregation_interval
                query = qry[key]
                par = self.ypar if self.traffic_type == cn.TRAFFIC_TYPE_MIV else 'total'
                df = fill_single_map_df(query, par, get_criteria())
                if len(df) == 0:
                    st.warning('Keine Daten gefunden, bitte ändern sie die Filter-Einstellungen')
                else:
                    show_map_columns(_title, df, par)
            elif self.result_type == 'Heatmap':
                key = 'heatmap_query_' + self.time_aggregation_interval
                query = qry[key]
                par = self.ypar if self.traffic_type == cn.TRAFFIC_TYPE_MIV else 'total'
                df = fill_heatmap_df(query, par, get_criteria())
                if len(df) == 0:
                    st.warning('Keine Daten gefunden, bitte ändern sie die Filter-Einstellungen')
                else:
                    if self.traffic_type == cn.TRAFFIC_TYPE_MIV:
                        _par = self.ypar
                        _y_ax_label = self.parameter_dic[self.ypar]
                    else:
                        _par = self.parameter_dic[self.ypar]
                        _y_ax_label = _par
                    df.rename(columns={'total': _par}, inplace=True)
                    show_heatmap(_title, df, _par, _y_ax_label)


        def show_grouped_result():
            def get_group_item_expression(item):
                if self.plot_groupby == 'weekday':
                    return cn.weekday_long_dic[item]
                elif self.plot_groupby == 'weekday_type':
                    return cn.weekday_type_dic[item]
                elif self.plot_groupby == 'month_from':
                    return cn.month_long_dic[item]
                elif self.plot_groupby == 'hour_from':
                    return cn.time_dic[item]
                elif self.plot_groupby == 'station_id':
                    return self.station_dic[item]
                else:
                    return str(item)

            def get_list(_criteria: str) -> list:
                _query = qry['group_list_query']
                _query = _query.format(self.plot_groupby, _criteria)
                _df = db.execute_query(_query, db.conn)
                return _df['grp'].tolist()
            
            # main
            st.markdown(f'## Anzahl {get_number_expression()} an Zählstelle')
            st.markdown(get_info())
            criteria = get_criteria()
            group_list = get_list(criteria)

            # clean list for nan values
            group_list = [x for x in group_list if str(x) != 'nan']
            for group_value in group_list:
                criteria_with_group = f' {criteria} and {self.plot_groupby} = {group_value}'
                title = get_group_item_expression(group_value)
                if self.result_type == 'Statistik':
                    st.markdown('### {}: {}'.format(cn.default_group_by_dic[self.plot_groupby],
                                                    get_group_item_expression(group_value)))

                    _query = qry['stats_query_' + self.time_aggregation_interval]
                    _df = fill_stat_df(_query, criteria_with_group)
                    st.table(_df)
                    st.markdown(tools.get_table_download_link(_df), unsafe_allow_html=True)
                else:
                    if self.result_type == 'Balkendiagramm':
                        _query = qry['barchart_query_' + self.time_aggregation_interval]
                        _df = fill_single_plot_df(_query, self.ypar, criteria_with_group)
                        if len(_df)>0:
                            if self.traffic_type == cn.TRAFFIC_TYPE_MIV:
                                _par = self.ypar
                                _y_ax_label = self.parameter_dic[self.ypar]
                            else:
                                _par = self.parameter_dic[self.ypar]
                                _y_ax_label = _par
                                _df.rename(columns={'total': _par}, inplace=True)
                            show_barchart(title, _df, _par, _y_ax_label)
                        else:
                            st.info("Es konnte keine Grafik erzeugt werden, bitte überprüfe die Kombination der Filter und Gruppier Optionen.")
                            break
                    elif self.result_type == 'Karte-Zählstelle':
                        _key = 'map_query_' + self.time_aggregation_interval
                        _query = qry[_key]
                        _df = fill_single_map_df(_query, self.ypar, criteria_with_group)
                        if self.traffic_type == cn.TRAFFIC_TYPE_MIV:
                            _par = self.ypar
                            _y_ax_label = self.parameter_dic[self.ypar]
                        else:
                            _par = self.parameter_dic[self.ypar]
                            _y_ax_label = _par
                            _df.rename(columns={'total': _par}, inplace=True)
                        show_map(title=title, data=_df, val_par=self.ypar)
                    elif self.result_type == 'Zeitreihe':
                        _key = 'timeseries_query_' + self.time_aggregation_interval
                        _key += ('' if self.marker_groupby == 'none' else '_direction')
                        _query = qry[_key]
                        _par = self.ypar if self.traffic_type == cn.TRAFFIC_TYPE_MIV else 'total'
                        _y_ax_label = self.parameter_dic[self.ypar]
                        _df = fill_single_map_df(_query, _par, criteria_with_group)
                        _df.rename(columns={'total': _par, 'direction_id': 'Richtung'}, inplace=True)
                        show_time_series(title, _df, _par, _y_ax_label)
                    elif self.result_type == 'Heatmap':
                        _query = qry['heatmap_query_' + self.time_aggregation_interval]
                        _par = self.ypar if self.traffic_type == cn.TRAFFIC_TYPE_MIV else 'total'
                        _df = fill_heatmap_df(_query, _par, criteria_with_group)
                        _y_ax_label = self.ypar
                        show_heatmap(title, _df, _par, _y_ax_label)

        def has_year_filter():
            """
            Allows to check whether there is a year filter set

            Returns
            -------
            bool
                True if a year filter is defined    
            """
            return not self.filter_by_date_flag and collections.Counter(list(self.year_filter)) != collections.Counter([self.year_list[0], self.year_list[-1]])
        
        def has_time_filter() -> bool:
            """
            Allows to check whether tehre is a filter set on the time interval

            Returns
            -------
            bool
                True if a filter is defined for time intervals    
            """
            #todo: allow to select time filter even if the aggregation is day. 
            return self.time_aggregation_interval == 'hour' and self.time_filter != (0, 23)
        
        def has_week_filter() -> bool:
            """
            Returns true if a filter is set for the week in year
            """
            return (self.week_filter > 0)
        
        def has_weekday_filter():
            """
            Returns true if a filter is set for the weekday in year
            """
            return (self.weekday_filter > 0)

        def has_weekday_type_filter():
            """
            Returns true if a filter is set for the weekday type (weekend/workday) in year
            """
            return (self.weekday_type_filter > 0)
        
        def has_month_filter():
            return (self.month_filter > 0)

        def get_criteria() -> str:
            criteria = ''
            if self.station_filter != 0:
                criteria = 'station_id = {}'.format(self.station_filter)
                if self.direction_filter != '0' and self.plot_groupby != 'station_id':
                    criteria += ' and direction_id = {}'.format(self.direction_filter)
                #if self.lane_filter != 'Alle':
                #    criteria += ' and lane_code = {}'.format(self.lane_filter)

            if self.filter_by_date_flag:
                logical = '' if criteria == '' else 'and'
                criteria += f" {logical} date(date) >= date('{self.date_filter[0]}') and date(date) <= date('{self.date_filter[1]}')"
            if has_week_filter():
                logical = '' if criteria == '' else 'and'
                criteria += f' {logical} week = {self.week_filter}'
            if has_month_filter():
                logical = '' if criteria == '' else 'and'
                criteria += f' {logical} month = {self.month_filter}'
            if has_year_filter():
                logical = '' if criteria == '' else 'and'
                criteria += f' {logical} year >= {self.year_filter[0]}'
                criteria += f' and year <= {self.year_filter[1]}'
            if has_time_filter():
                logical = '' if criteria == '' else ' and '
                criteria += f'{logical}hour_from >= {self.time_filter[0]}'
                criteria += f' and hour_from <= {self.time_filter[1]}'
            if has_weekday_filter():
                logical = '' if criteria == '' else ' and '
                criteria += f'{logical}weekday = {self.weekday_filter}'
            if has_weekday_type_filter():
                logical = '' if criteria == '' else ' and '
                criteria += f'{logical} weekday {"<6" if self.weekday_type_filter == 1 else">5"}'
            
            logical = '' if criteria == '' else ' and '
            criteria += f'{logical} traffic_type = {self.traffic_type}'
            return criteria

        def show_barchart(title: str, data: pd.DataFrame, val_par: str, y_axis_title: str):
            tooltips = ['station_name', val_par] if 'station_name' in data.columns else []
            if self.yax_max == self.yax_min:
                scy = alt.Scale()
            else:
                scy = alt.Scale(domain=(self.yax_min, self.yax_max))
            bar = alt.Chart(data).mark_bar().encode(
                x=alt.X(f'{self.marker_groupby}:O',
                        sort=self.sort_dic[self.marker_groupby],
                        title=cn.default_group_by_dic[self.marker_groupby]),
                y=alt.Y(f'{val_par}:Q', title=y_axis_title, scale=scy),
                tooltip=tooltips
            )
            # not sure why the mean is sometime lower than the minimum bar mean
            # rule = alt.Chart(data).mark_rule(color='red').encode(
            # y='mean({}):Q'.format(val_par)
            chart = bar.properties(width=self.plot_width, height=self.plot_height, title=title)
            st.altair_chart(chart)
            tools.log('end barchart')

        def show_heatmap(title: str, data: pd.DataFrame, val_par: str, y_axis_title: str):
            tooltip = ['station_name', val_par]
            if self.yax_max == 0:
                scale = alt.Scale(scheme="bluegreen")
            else:
                scale = alt.Scale(scheme="bluegreen", domain=[self.yax_min, self.yax_max])

            hmap = alt.Chart(data).mark_rect().encode(
                x=alt.X(f''
                        f'{self.marker_groupby}:O',
                        title=cn.default_group_by_dic[self.marker_groupby],
                        sort=self.sort_dic[self.marker_groupby]),
                y=alt.Y(f'station_name:O',
                        title='Zählstelle'),
                color=alt.Color(f'{val_par}:Q', scale=scale),
                tooltip=tooltip
            )

            # not sure why the mean is sometime lower than the minimum bar mean
            # rule = alt.Chart(data).mark_rule(color='red').encode(
            # y='mean({}):Q'.format(val_par)
            chart = hmap.properties(width=self.plot_width, height=self.plot_height, title=title)
            st.altair_chart(chart)
            tools.log('end barchart')

        def show_map(title: str, data: pd.DataFrame, val_par: str):
            """
            Draws a map plot using deckgl
            """

            midpoint = (np.average(data['long']), np.average(data['lat']))
            tooltip_html = f"Zählstelle: {{station_name}}</br>Durchschnitt ({val_par}): {{{val_par}}}"
            data = data.reset_index()
            if title != 'Alle':
                st.markdown(title)
            data['icon_data'] = None
            for i in data.index:
                data['icon_data'][i] = cn.icon_data
            layer = pdk.Layer(
                type='IconLayer',
                data=data,
                get_icon='icon_data',
                pickable=True,
                size_scale=20,
                get_position="[long, lat]",
            )
            view_state = pdk.ViewState(
                longitude=midpoint[0], latitude=midpoint[1], zoom=11, min_zoom=1, max_zoom=100, pitch=0, bearing=0
            )
            r = pdk.Deck(
                map_style="mapbox://styles/mapbox/light-v10",
                layers=[layer],
                initial_view_state=view_state,
                tooltip={
                    "html": tooltip_html,
                    "format": '.1f',
                    "style": {'fontSize': cn.TOOLTIP_FONTSIZE,
                              "backgroundColor": cn.TOOLTIP_BACKCOLOR,
                              "color": cn.TOOLTIP_FORECOLOR},
                }
            )
            st.pydeck_chart(r)
        
        def show_map_columns(title: str, data: pd.DataFrame, val_par: str):
            """
            Draws a map plot using deckgl
            """

            midpoint = (np.average(data['long']), np.average(data['lat']))
            data = data[['long','lat','total']].reset_index()
            if title != 'Alle':
                st.markdown(title)
            layer = pdk.Layer(
                type='HeatmapLayer',
                data=data,
                radius=50,
                get_position="[long, lat]",
                elevation_scale=10,
                elevation_range=[0, 1000],
                pickable=True,
                extruded=True,
                wireframe=True,
                get_weight="total"
            )
            view_state = pdk.ViewState(
                longitude=midpoint[0], latitude=midpoint[1], zoom=11, min_zoom=1, max_zoom=100, pitch=50, bearing=0
            )
            r = pdk.Deck(
                map_style="mapbox://styles/mapbox/light-v8",
                layers=[layer],
                initial_view_state=view_state,
                tooltip={
                    "html": "Anzahl: {total}",
                    "format": '.1f',
                    "style": {'fontSize': cn.TOOLTIP_FONTSIZE,
                              "backgroundColor": cn.TOOLTIP_BACKCOLOR,
                              "color": cn.TOOLTIP_FORECOLOR},
                }
            )
            st.pydeck_chart(r)

        def get_time_format(start_date, end_date):
            """
            Returns an appropriate date format to be shown on a time series plot axis. see:
            https://github.com/d3/d3-time-format#locale_format
            """

            td = end_date.to_pydatetime() - start_date.to_pydatetime()
            td_days = td.total_seconds() / (3600 * 24)
            if td_days < 3:
                return '%x %H:%M'
            elif td_days < 366:
                return "%x"
            elif td_days < 5 * 366:
                return "%b %y"
            else:
                return "%y"

        def show_time_series(title: str, df: pd.DataFrame, par: str, y_lab: str):
            """
            Plots a time series plot. for time series plots the marker group by parameter is automatically set to the
            station.

            Parameters:
            -----------
            :param title:
            :param df:
            :param par:
            :return:
            """

            _x_lab = ''
            df['time'] = pd.to_datetime(df['time'])
            min_dat = df['time'].min()
            max_dat = df['time'].max()
            time_format = get_time_format(min_dat, max_dat)
            _color_column = 'station_name' if self.marker_groupby == 'none' else 'Richtung'

            if self.yax_max == self.yax_min:
                scy = alt.Scale()
            else:
                scy = alt.Scale(domain=(self.yax_min, self.yax_max))

            if self.moving_average_days > 0:
                line = alt.Chart(df, title=title).mark_line(point=False, clip=True
                                                            ).transform_window(
                    rolling_mean='mean({})'.format(par),
                    frame=[-self.moving_average_days / 2, self.moving_average_days]
                ).encode(
                    x=alt.X('time:T',
                            axis=alt.Axis(title=_x_lab)),
                    # https://github.com/d3/d3-time-format#locale_format
                    y=alt.Y('rolling_mean:Q',
                            scale=scy,
                            axis=alt.Axis(title=y_lab)
                            ),
                    color=alt.Color(_color_column,
                                    scale=alt.Scale(scheme=cn.color_schema)
                                    ),
                )
            else:
                line = alt.Chart(df).mark_line(point=True, clip=True
                                               ).encode(
                    x=alt.X(f'time:T',
                            axis=alt.Axis(title=_x_lab, labelAngle=30, format=time_format)),
                    y=alt.Y('{}:Q'.format(par),
                            scale=scy,
                            axis=alt.Axis(title=y_lab)
                            ),
                    color=alt.Color(_color_column,
                                    scale=alt.Scale(scheme=cn.color_schema)
                                    ),
                )

            points = alt.Chart(df).mark_point(
            ).encode(
                x=alt.X('time:T',
                        axis=alt.Axis(title=_x_lab)),
                y=alt.Y('{}:Q'.format(par),
                        scale=scy,
                        axis=alt.Axis(title=y_lab)
                        ),
                color=alt.Color(_color_column,
                                scale=alt.Scale(scheme=cn.color_schema)
                                ),
                tooltip=['station_name', _color_column, 'time', par],
                opacity=alt.value(0.3)
            )
            chart = (points + line).properties(width=self.plot_width, height=self.plot_height, title=title)
            st.altair_chart(chart)
            # st.table(df)

        # --------------------------------------------------------------------------------------------------------------
        # main program
        # --------------------------------------------------------------------------------------------------------------

        if self.plot_groupby == 'none':
            show_single_result()
        else:
            show_grouped_result()
