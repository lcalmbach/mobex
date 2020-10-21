import streamlit as st
import sys
import numpy as np
import traffic
import config as cn
import tools

tr = traffic.Traffic('Info Datensatz')
title_placeholder = st.sidebar.empty()
st.sidebar.markdown('<small> MobEx.bs version: {}</small>'.format(traffic.__version__), unsafe_allow_html=True)

tr.traffic_type = st.sidebar.selectbox('Verkehrsart', index=0,
                   options=list(cn.traffic_type_dic.keys()),
                   format_func=lambda x: cn.traffic_type_dic[x])
title_placeholder.markdown(f'# {cn.logo_dic[tr.traffic_type]} <span style="color:steelblue">Mobility Explorer BS</span>', unsafe_allow_html=True)

menu = st.sidebar.radio(label='', index=0, options=cn.menu_list)
print(menu)
if menu == 'Info Datensatz':
    tr.show_dataset_info()
    tr.show_about_box()
elif menu == 'Statistik':
    tr.result_type = menu
    tr.show_sidebar_controls()
    tr.show_results()
elif menu == 'Grafiken':
    tr.result_type = st.sidebar.selectbox("Grafik-Typ", index=0, options=cn.plot_type_list)
    tr.show_sidebar_controls()
    tr.show_results()

# st.write(sys.version_info)
tr.show_help_icon()
