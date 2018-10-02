import pandas as pd
import numpy as np

import psycopg2 as pg2
import sqlalchemy
import s3fs

import julian
import datetime
import calendar
import time

import gdal
import os
import csv

from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib as mpl


def get_meta_data(name='weather', loc='localhost'):
    conn = pg2.connect(dbname=name, host=loc)
    cur = conn.cursor()
    conn.autocommit = True
    query = f"""SELECT station_id, latitude, longitude 
                 FROM station_metadata;"""
    cur.execute(query)
    data = cur.fetchall()
    conn.close()
    return data


def get_weather_for_one_day(table_name, day, name='weather', loc='localhost'):
    """Get the Weather Data for all stations for one day
    """
    conn = pg2.connect(dbname=name, host=loc)
    cur = conn.cursor()
    conn.autocommit = True

    get_weather_command = f"""SELECT * FROM {table_name} 
                            WHERE measurement_date 
                            BETWEEN '{day}' AND '{day}';"""

    cur.execute(get_weather_command)
    weather = cur.fetchall()

    df = prepare_weather_data_for_merge(weather)
    df = pivot_weather_data_frame(df)
    conn.close()
    return df


def prepare_weather_data_for_merge(df):
    """INPUTS:
    Self, and a DataFrame, an SQL QUERY
    OUTPUTS:
    A Data frame with index of 'measurement_date',

    """
    wdf = pd.DataFrame(df, columns=[
        'index', 'station_id', 'measurement_date',
        'measurement_type', 'measurement_flag'])
    wdf = wdf.set_index('measurement_date')
    wdf.drop(columns=['index'], inplace=True)
    wdf.index = pd.to_datetime(wdf.index)
    wdf['measurement_flag'] = wdf['measurement_flag'].astype(float)
    return wdf


def pivot_weather_data_frame(df):
    """INPUTS: Self, df
    OUTPUTS:
    A Pandas DataFrame with columns:
    PRCP|SNOW|SNWD|TMAX|TMIN with values in corresponding "measurment_flag" as floats
    """
    pivoted = pd.pivot_table(df, index=['station_id', 'measurement_date'],
                             columns='measurement_type',
                             values='measurement_flag')

    #grouped_by_day = pivoted.groupby('measurement_date')

    # return get_julian_day_column(grouped_by_day)
    return pivoted


def load_metadata(meta_data_path='/Users/Berzyy/plant_forecast/preloaded_data/ghcnd-stations.txt'):
    """Loads the ghcnd-stations.txt into a Pandas DataFrame.
    OUTPUT:
    A Pandas Data frame with columns:
    station_id|latitude|longitude|elevation|state
    Returns self
    """
    df = pd.read_csv(meta_data_path,
                     sep='\s+',
                     usecols=[0, 1, 2, 3, 4],
                     # Missing elevation is noted as -999.9
                     na_values=[-999.9],
                     header=None,
                     names=['station_id', 'latitude',
                            'longitude', 'elevation', 'state'],
                     engine='python')

    idfinder = station_id_lookup(df)

    return df, idfinder


def station_id_lookup(df):
    """Takes in a data frame
    returns a dictionary with the keys as station_id,
    lat, long, elevation, and state as values.
    """
    station_dict = defaultdict()
    values = df.values
    for row in values:
        stationid = row[0]
        data = row[1:]
        station_dict[stationid] = data
    return station_dict


def make_jpg_files(idi, start=2009, end=2018):
    path_to_folder = '../pics/'

    for year in range(start, end):
        table_name = 'w_' + str(year)[-2:]
        print(f"Fetching from table {table_name}")

        first_day = datetime.date(year=year, day=1, month=1)
        last_day = datetime.date(year=year, day=31, month=12)

        rng = pd.date_range(end=last_day, start=first_day, freq='D')

        for day in rng:
            start = time.time()
            query_day = str(day.date())
            df = get_weather_for_one_day(table_name, query_day)

            lat, longi, var, precip = make_plot_lists(idi, df)

            path = path_to_folder + query_day + '.jpg'
            plot_tool(lat, longi, var, precip, path, query_day)
            print(
                f'Fetched Weather Data for {query_day} in {time.time()-start:.2f} seconds!')

    return None


def make_plot_lists(idi, df):
    variables = []
    latitudes = []
    longitudes = []
    precips = []
    for station_index in range(0, len(df)):
        station_id, high_temp, precip = df.index[station_index][
            0], df.TMAX[station_index], df.PRCP[station_index]

        latitude = idi[station_id][0]
        longitude = idi[station_id][1]

        latitudes.append(latitude)
        longitudes.append(longitude)
        variables.append(high_temp)
        precips.append(precip)

    return latitudes, longitudes, variables, precips

#cmap, norm = mpl.colors.from_levels_and_colors([0, 2, 5, 6], ['red', 'green', 'blue'])


def plot_tool(lat, longi, var, precip, path, day):
    fig, ax = plt.subplots(figsize=(15, 8))
    cmap = plt.get_cmap('magma', 21)
    norm = mpl.colors.Normalize(vmin=-457, vmax=455)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    fig.colorbar(sm, ticks=np.linspace(-457, 455, 21)),

    ax.scatter(longi, lat, alpha=0.3, c=var, cmap=cmap, s=50, norm=norm)
    ax.scatter(longi, lat, s=precip, alpha=.05, color='skyblue')
    # ax.set_ylim(20, 60)
    # ax.set_xlim(-140, -50)
    ax.set_title(day, fontsize=20)

    fig.savefig(path)

    return None


if __name__ == '__main__':
    xmeta_data, idi = load_metadata()
    make_jpg_files(idi)
    pass
