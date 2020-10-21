import streamlit as st
import database as db
import tools
import config as cn
import etl

# menu item info and action strings
menu = {
    '0': 'Import stations',
    '1': 'Import MIV traffic records', 
    '2': 'Import slow traffic records', 
    '3': 'Compact database file', 
    '4': 'insert missing rows',
    '5': 'Insert all missing rows'
}
menu_desc = {
    '0': f"""# Import stations
    - Imports all MIV records of the current year to the source database: {cn.source_miv_file_name} -> miv_traffic_source
    - converts date columns
    - writes data to the local test database.
    - before tranferring the records to the prod database, app.py should be tested. Run ```streamlit run app.py``` from the terminal.""", 
    '1': '### Import MIV traffic records',
    '2': '### Import slow traffic records',
    '3': '### Compact database',
    '4': '### insert missing rows',
    '5': '### insert all missing rows'
}

st.sidebar.markdown('# 🗄️<span style="color:steelblue">DB Tools</span>', unsafe_allow_html=True)
st.sidebar.markdown('<small>for MobEx.bs</small>', unsafe_allow_html=True)
st.sidebar.markdown('### Menu')

menu_item = st.sidebar.selectbox(label='', index=0, options=list(menu.keys()), format_func=lambda x: menu[x])
year = st.sidebar.selectbox(label='Year', index=0, options=range(2015, 2021))
st.write(menu_desc[menu_item])

db.init()
if st.sidebar.button('execute'):
    if menu_item == '0':
        etl.stations_import()
    elif menu_item == '1':
        etl.import_traffic(year, cn.TRAFFIC_TYPE_MIV)
    elif menu_item == '2':
        etl.import_traffic(year, cn.TRAFFIC_TYPE_SLOW)
    elif menu_item == '3':
        etl.compact_db()
    elif menu_item == '4':
        etl.insert_missing_rows(year)
    elif menu_item == '5':
        etl.insert_all_missing_rows()
    
