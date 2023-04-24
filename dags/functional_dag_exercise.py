from datetime import datetime
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import pandas as pd


""" Dag that downloads data and returns number of accidents per year."""
with DAG(
        'accidents_count',
        default_args={'schedule_interval': "0 * * * *", "start_date": datetime(2023, 1, 13)}
) as dag:

    path = Variable.get('path', default_var=None)

    # Get data and write to csv file
    @task(task_id="get_data")
    def get_data():

        data_url = "https://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource" \
              "/8673744e-53f2-42d1-9d05-4e412bd55c94/download/monroe-county-crash-data2003-to-2015.csv"
        response = requests.get(data_url)

        with open(f"{path}accident_data.csv", "w") as opened_file:
            opened_file.write(response.text)

    get_data = get_data()

    # Count the number of accidents per year
    def count_accidents_per_year():

        df = pd.read_csv(f"{path}accident_data.csv")
        grouped_by_year = df.groupby('Year').count()
        return grouped_by_year.to_dict()

    # Display the results
    @task(task_id="display_results")
    def display_results(data):
        print(data)

    display_results = display_results(count_accidents_per_year())

    get_data >> display_results












