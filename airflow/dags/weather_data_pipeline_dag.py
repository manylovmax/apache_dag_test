import urllib.request
import pendulum
import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow.decorators import  dag, task

JSON_WEATHER_DATA_FILE_PATH = '/tmp/weather_data.json'
# JSON_WEATHER_DATA_FILE_PATH = os.getcwd() + '/weather_data.json'
CSV_FILE_PATH = os.getcwd() + '/processed_weather_data.csv'


@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["weather", "test"],
)
def weather_data_pipeline_dag():

    @task(task_id='download_data')
    def download_data_callable():
        with urllib.request.urlopen('http://api.openweathermap.org/data/2.5/weather?q=London&appid=2ccd190a185aa973681572f0e6d1b9e0') as file:
            with open(JSON_WEATHER_DATA_FILE_PATH, "w") as new_file:
                bytes = file.read()
                new_file.write(bytes.decode())

    download_data = download_data_callable()

    @task(task_id='process_data')
    def process_data_callable():
        def kelvin_to_celsius(kelvin):
             return kelvin - 273.15
        
        def append_data_to_csv(df):
            exists = os.path.isfile(CSV_FILE_PATH)

            if exists:
                old_df = pd.read_csv(CSV_FILE_PATH)
                new_df = pd.concat([old_df, df])
                new_df.to_csv(CSV_FILE_PATH, index=False)
            else:
                df.to_csv(CSV_FILE_PATH, index=False)

             

        with open(JSON_WEATHER_DATA_FILE_PATH, "r") as json_file:
            json_data = json.load(json_file)
        

        df = pd.json_normalize(json_data)
        df['main.temp'] = kelvin_to_celsius(json_data['main']['temp'])
        df['main.feels_like'] = kelvin_to_celsius(json_data['main']['feels_like'])
        df['main.temp_min'] = kelvin_to_celsius(json_data['main']['temp_min'])
        df['main.temp_max'] = kelvin_to_celsius(json_data['main']['temp_max'])
        
        append_data_to_csv(df)



    process_data = process_data_callable()

    @task(task_id='save_data')
    def save_data_callable():
        df = pd.read_csv(CSV_FILE_PATH)
        table = pa.Table.from_pandas(df)
        parquet_file_path = os.getcwd() + '/weather.parquet'
        pq.write_table(table, parquet_file_path)
            

    save_data = save_data_callable()

    download_data >> process_data >> save_data

weather_data_pipeline_dag()
