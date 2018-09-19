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

import matplotlib.pyplot as plt


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


def get_weather_for_one_day(name='weather', loc='localhost'):
    """Get the Weather Data for all stations for one day
    """
    conn = pg2.connect(dbname=name, host=loc)
    cur = conn.cursor()
    conn.autocommit = True

    get_weather_command = """SELECT * FROM w_00;"""

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

    grouped_by_day = pivoted.groupby('measurement_date')

    # return get_julian_day_column(grouped_by_day)
    return grouped_by_day
