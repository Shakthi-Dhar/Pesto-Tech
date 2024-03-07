from airflow import DAG, models, utils
from datetime import datetime, timedelta, date
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


import json
import pandas_gbq
import pandas as pd
import requests


def write_to_bigquery(dataframe, project_id, dataset_id, table_id):
    """
    Writes a Pandas DataFrame to BigQuery.
    Args:
        dataframe: Pandas data frame with the required data
        project_id: Bigquery project id where the table is present
        dataset_id: Dataset in BigQuery where the data is to be added
        table_id: Table in BigQuery where the data is to be appended
    Returns:
        None
    """
    # print(dataframe)
    if len(dataframe.index) == 0:
        # print(dataframe.info())
        return None
    table = dataset_id + "." + table_id
    try:
        pandas_gbq.to_gbq(dataframe, table, project_id, if_exists="append")
    except Exception as error_uploading:
        print(f"Error while uploading data to Bigquery: {error_uploading}")


def ads_data_mining(report_date):
    """
    Function to read the data from ads API for the specific report date.

    Source: THE API FOR AdvertiseX

    Args:
        report_date: The date of activities we want to scrape/extract
    """
    data = []
    try:
        request_response = requests.get(
            f"API for AdvertiseX requirement for the report date: {report_date}",
            timeout=300,
        )
    except Exception as error_request:
        print(f"Error occured while hitting the API: {error_request}")
        return 0

    for row in request_response:
        try:
            dict_data = {
                "campaign": row["campaign"],
                "ad_group": row["ad_group"],
                "segments": row["segments"],
                "metrics": row["metrics"],
                "customer": row["customer"],
                "report_date": row["report_date"],
            }
        except Exception as error_response:
            print(f"Error occured while reading the API response: {error_response}")

        data.append(dict_data)

    data_frame = pd.DataFrame(data)
    data_frame = data_frame.astype(str)
    write_to_bigquery(data_frame, "PROJECT_ID", "DATASET_ID", "TABLE_ID")


def main(**kwargs):
    """
    Main function to trigger the ads data mining function from the last extracted date
    """
    report_date = datetime.strptime(str(kwargs["ds"]), "%Y-%m-%d").date()
    current_date = datetime.now().date()

    while report_date <= current_date:
        print(f"Fetching the data for: {report_date}")
        ads_data_mining(report_date)


default_dag_args = {
    "start_date": utils.dates.days_ago(1),
    "retries": 0,
}

with models.DAG(
    "AdvertiseX Data Mining",
    schedule_interval="0 0 * * *",
    default_args=default_dag_args,
) as dag:

    main = PythonOperator(
        task_id="main",
        python_callable=main,
        provide_context=True,
    )

    # dummy task to start the DAG
    start_task = EmptyOperator(task_id="start")

    # dummy task to end the DAG
    end_task = EmptyOperator(task_id="end")

    (start_task >> main >> end_task)
