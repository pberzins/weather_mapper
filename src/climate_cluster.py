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
import imageio


def get_one_station(station_id, state_list):
    """Takes in one station and gets the average information
    per month. 
    """
    conn = pg2.connect(dbname='weather', host='localhost')
    cur = conn.cursor()
    conn.autocommit = True

    get_weather_command = f"""SELECT * FROM w_00 
                            WHERE station_id = '{station_id}';"""

    cur.execute(get_weather_command)
    station_data = cur.fetchall()
    station_df = helper.prepare_weather_data_for_merge(station_data)
    pivoted_df = helper.pivot_weather_data_frame(station_df)
    conn.close()
    return pivoted_df


def average_by_month(station_df):
    """Takes in a dataframe, averages everythig by month:
    """
    df = station_df.reset_index(level=0)
    month_groups = df.groupby(by=[df.index.month])
    return month_groups.mean()
