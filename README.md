# MobEx
version 0.4.0

MobEx is a Mobility explorer app for Basel/Switzerland. Information on the functionality of the app can be found in the help file of the application. This readme focuses on how to build this applications no your machine.

## Steps to build the application
1. Clone the git project
```
> mkdir mobex
> cd mobex
> git clone https://github.com/lcalmbach/mobex.git
```
2. Create virtual python environemnt and load required python packages
```
> python -m venv env
> env\scripts\activate.bat
> pip install -r requirements.txt 
> 
```
3. Initialize the database   
The traffic database is too large to upload on git. Therefore an empty database contained all required tables and views is provided. A second app named app_etl can be used to fill and keep the data of the database up to date. The database already includes all helper data such as codes and stations, but the fact table traffic_fact includes just data for 1 station and month.

```
> cd mobex
> ren traffic_empty.sqlite3 traffic.sqlite3
> cd mobex
> streamlit run app_etl.py
```

In the app_etl app shown bleow select import miv  menu item and press the execute button. repeat the procedure for each year (currently 2015 to 2020 is available). REpeat the procedure for the slow traffic dataset (bycicles and pedestrians)

![](\static\images\app_py_screenshot.png)

4. Run MobEx
```
> Ctrl-C # stop the etl-app in case it is running
> streamlit run app.py   
```




