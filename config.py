"""
Holds all constants used by the app MobEx.
sql statements are placed in a separate file (query.py)
"""

INFO_IMAGE_FILE = {1:'static/images/traffic_bs.png',2:'static/images/traffic_bs.png',3:'static/images/traffic_bs.png'}
INFO_FILE = "./info.md"
DATABASE_FILE = './traffic.sqlite3'

ABOUT_TEXT = """
        Diese Applikation wurde von [Lukas Calmbach](mailto:lcalmbach@gmail.com) 
        in [Python](https://www.python.org/) entwickelt. Als Komponenten wurden [Streamlit](https://streamlit.io/), [sqlite](https://sqlite.org/index.html) 
        und [Altair](https://altair-viz.github.io/) eingesetzt. Der Quellcode ist auf 
        [github](https://github.com/lcalmbach/verkehr) publiziert.
        """

category = {'direction': 1, 'weekday': 2, 'vehicule_type': 3, 'slow_traffic_type': 4}
weekday_long_dic = {0: 'Alle', 1: 'Montag', 2: 'Dienstag', 3: 'Mittwoch', 4: 'Donnerstag', 5: 'Freitag', 6: 'Samstag',
                    7: 'Sonntag'}
weekday_short_dic = {0: 'Alle', 1: 'Mo', 2: 'Di', 3: 'Mi', 4: 'Do', 5: 'Fr', 6: 'Sa', 7: 'So'}
weekday_short_list = list(weekday_short_dic.values())
weekday_type_dic = {0: 'Alle', 1: 'Werktage', 2: 'Wochenende'}
time_filter_list = ['keine', 'Stunde', 'Datum', 'Woche', 'Monat', 'Jahr']
weekday_long_list = list(weekday_long_dic.values())
time_dic = {0: '00-01h', 1: '01-02h', 2: '02-03h', 3: '03-04h', 4: '04-05h', 5: '05-06h', 6: '06-07h', 7: '07-08h',
            8: '08-09h', 9: '09-10h', 10: '10-11h', 11: '11-12h',
            12: '12-13h', 13: '13-14h', 14: '14-15h', 15: '15-16h', 16: '16-17h', 17: '17-18h', 18: '18-19h',
            19: '19-20h', 20: '20-21h', 21: '21-22h', 22: '22-23h', 23: '23-00h'}

month_short_dic = {0: 'Alle', 1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Okt',
                   11: 'Nov', 12: 'Dez'}
month_long_dic = {0: 'Alle', 1: 'Januar', 2: 'Februar', 3: 'März', 4: 'April', 5: 'Mai', 6: 'Juni', 7: 'Juli', 8: 'August',
                  9: 'September', 10: 'Oktober', 11: 'November', 12: 'Dezember'}
default_group_by_dic = {'none': 'keine', 'year': 'Jahr', 'month': 'Monat', 'weekday': 'Wochentag', 'weekday_type': 'Werktage/Wochenende', 
                'week': 'Kalenderwoche','hour_from': 'Tageszeit', 'direction_id': 'Fahrtrichtung', 'station_id': 'Zählstelle', 
                }
ts_marker_group_by_dic = {'none': 'keine', 'direction_id': 'Fahrtrichtung'} 
ts_plot_group_by_dic = {'none': 'keine', 'year': 'Jahr', 'station_id': 'Zählstelle'} 
                

logo_dic = {1: "🚗", 2: "🚲", 3: "🚶🏻"}
traffic_type_dic = {1: 'Motorisierter Individualverkehr', 2: 'Langsamverkehr'}
parameter_dic = {
                1: {'total': 'Alle Fahrzeugtypen', 'mr': 'Motorräder', 'pw': 'Personenwagen',
                 'lief': 'Lieferwagen', 'pw_lief': 'Personenwagen und Lieferwagen', 'lw': 'Lastwagen',
                  'sattelzug': 'Sattelzüge', 'lw_sattelzug': 'Lastwagen und Sattelzüge','bus': 'Busse', 'andere': 'Andere Fahrzeuge'},
                2: {'total': 'Fahrräder'},
                3: {'total': 'Fussgänger'}
                }
TRAFFIC_TYPE_MIV = 1
TRAFFIC_TYPE_SLOW = 2
traffic_type_dic = {1: 'MIV', 2: 'Velo', 3: 'Fussgänger'}
timeseries_time_aggregation_dic = {'day': 'Pro Tag', 'hour': 'Pro Stunde', 'week': 'Pro Woche'}
default_time_aggregation_dic = {'day': 'Pro Tag', 'hour': 'Pro Stunde'}
plot_type_list = ['Balkendiagramm', 'Karte-Zählstelle', 'Karte-Verkehr', 'Zeitreihe', 'Heatmap']
menu_list = ['Info Datensatz', 'Statistik', 'Grafiken']
months_list = ['Alle', 'Jan', 'Feb', 'Mrz', 'Apr', 'Mai', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dez']
days_list = ['Alle', 'Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
time_intervals_list = [] #tools.get_time_intervals()
def_plot_width = 800
def_plot_height = 400
STATION_NAME_COLUMN = 'SiteName'
color_schema = "set1"  # https://vega.github.io/vega/docs/schemes/#reference
color_schema_alt = "tableau10"  # https://vega.github.io/vega/docs/schemes/#reference
symbol_size = 60
date_time_column = 'DateTimeFrom'
USER_MANUAL_LINK: str = 'https://lcalmbach.github.io/mobex-doc/'
HELP_ICON: str = 'https://img.icons8.com/offices/30/000000/help.png'

# sql queries
source_miv_file_name = 'https://data-bs.ch/mobilitaet/{}_MIV_Class_10_1.csv'
source_slow_file_name = 'https://data-bs.ch/mobilitaet/{}_Velo_Fuss_Count.csv'
SOURCE_STATION_FILE_NAME = 'https://data.bs.ch/explore/dataset/100038/download/?format=csv&timezone=Europe/Berlin&lang=de&use_labels_for_header=true&csv_separator=%3B'

TOOLTIP_FONTSIZE = 'small'
TOOLTIP_BACKCOLOR = 'white'
TOOLTIP_FORECOLOR = 'black'

icon_data = {
            "url": "https://img.icons8.com/plasticine/100/000000/marker.png",
            "width": 128,
            "height": 128,
            "anchorY": 128
            }
