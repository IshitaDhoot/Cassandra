import csv
import datetime
import random
import station_pb2_grpc, station_pb2, grpc
import sys
import gzip
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
import pandas as pd
from datetime import datetime


def simulate_sensor(station):
    spark = SparkSession.builder.appName("sensor-data").getOrCreate()
    station_csv_path = f"{station}.csv.gz"
    station_df = spark.read.format("csv").option("compression", "gzip").load(station_csv_path)
    weather = station_df.filter(substring("_c1", 1, 4) == "2022").toPandas()
                     
    df_tmax = weather[weather['_c2'] == 'TMAX'].rename(columns={'_c1': 'DATE', '_c3': 'TMAX'}).drop(['_c0','_c2', '_c4', '_c5', '_c6', '_c7'], axis=1)
    df_tmin = weather[weather['_c2'] == 'TMIN'].rename(columns={'_c1': 'DATE', '_c3': 'TMIN'}).drop(['_c0', '_c2', '_c4','_c5', '_c6', '_c7'], axis=1)
    df_tmax_tmin = pd.merge(df_tmax, df_tmin, on=['DATE'])

    # Loop over each day and send temperature data to server
    for _, row in df_tmax_tmin.iterrows():
        date = row['DATE']
        date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        tmax = int(row['TMAX'])
        tmin = int(row['TMIN'])
        request = station_pb2.RecordTempsRequest(station=station, date=date, tmin=tmin, tmax=tmax)
        try:
            response = stub.RecordTemps(request)
            if response.error:
                print(f"Error: {response.error}")
        except (ValueError) as e:
            print(f"Error: {str(e)}")
            break

            

addr = f"127.0.0.1:5440"
channel = grpc.insecure_channel(addr)
stub = station_pb2_grpc.StationStub(channel)

for station in ["USW00014837", "USR0000WDDG", "USW00014898", "USW00014839"]:
    simulate_sensor(station)
    r = stub.StationMax(station_pb2.StationMaxRequest(station=station))
    if r.error:
        print(r.error)
    else:
        print(f"max temp for {station} is {r.tmax}")
