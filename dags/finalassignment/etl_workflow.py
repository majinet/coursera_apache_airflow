import os
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import tarfile
import pandas as pd
import numpy as np

with DAG(
    "ETL_toll_data",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "owner": "DataOps",
        "start_date": datetime(2023, 2, 5),
        "email": ["majinetudacitylearn@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description="Apache Airflow Final Assignment",
    schedule=timedelta(days=1),
) as dag:

    def unzip_data(filename: str) -> None:
        """
        unzip data

        :param: filename
        """

        # open file
        file = tarfile.open(filename)
        # extracting file
        file.extractall('/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/')
        file.close()

    def extract_data_from_csv(filename: str) -> None:
        """
        extract csv and save to new file

        :param: filename
        """

        df = pd.read_csv(filename, index_col=False,
                            names=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'],
                            usecols=[0, 1, 2, 3], header=0)

        df.to_csv('/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/temp_data/csv_data.csv', index=False)

    def extract_data_from_tsv(filename: str) -> None:
        """
        extract tsv and save to new file

        :param: filename
        """

        df = pd.read_table(filename, index_col=False,
                            names=['Number of axles', 'Tollplaza id', 'Tollplaza code'],
                            usecols=[4, 5, 6], header=0)

        df.to_csv('/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/temp_data/tsv_data.csv', index=False)

    def extract_data_from_fixed_width(filename: str) -> None:
        """
        extract fixed width txt and save to new file

        :param: filename
        """

        df = pd.read_fwf(filename, widths=[6, 25, 7, 9, 10, 4, 6])
        df = df.iloc[:, [5, 6]]

        df.columns = ['Type of Payment code', 'Vehicle Code']

        df.to_csv('/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/temp_data/fixed_width_data.csv', index=False)

    def consolidate_data(filename1: str, filename2: str, filename3: str) -> None:
        """
        combining data

        :param: filename
        """
        print("start consolidation")
        df1 = pd.read_csv(filename1, index_col=False,
                         names=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'],
                         header=0)

        df2 = pd.read_csv(filename2, index_col=False,
                           names=['Number of axles', 'Tollplaza id', 'Tollplaza code'],
                           header=0)

        df3 = pd.read_csv(filename3, index_col=False,
                            names=['Type of Payment code', 'Vehicle Code'],
                            header=0)

        df = pd.concat([df1, df2, df3], axis=1, ignore_index=True)
        df.columns = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code']

        df.to_csv('/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/temp_data/extracted_data.csv', index=False)

    def transform_data(filename: str) -> None:
        """
        extract fixed width txt and save to new file

        :param: filename
        """

        df = pd.read_csv(filename, index_col=False,
                names= ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code'],
                header=0)

        df['Vehicle type'] = df['Vehicle type'].str.upper()

        df.to_csv('/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/temp_data/transformed_data.csv', index=False)

    # create task pipeline
    task_unzip = PythonOperator(
        task_id="unzip_data",
        python_callable=unzip_data,
        op_kwargs={'filename': '/mnt/d/learning/ibm/dataops/build_airflow_workflow/data/tolldata.tgz'},
    )

    task_extract_csv = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_data_from_csv,
        op_kwargs={'filename': '/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/vehicle-data.csv'},
    )

    task_extract_tsv = PythonOperator(
        task_id="extract_tsv",
        python_callable=extract_data_from_tsv,
        op_kwargs={'filename': '/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/tollplaza-data.tsv'},
    )

    task_extract_txt = PythonOperator(
        task_id="extract_txt",
        python_callable=extract_data_from_fixed_width,
        op_kwargs={'filename': '/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/payment-data.txt'},
    )

    task_consolidate_data = PythonOperator(
        task_id="consolidate_data",
        python_callable=consolidate_data,
        op_kwargs= {'filename1': '/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/temp_data/csv_data.csv',
                    'filename2': '/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/temp_data/tsv_data.csv',
                    'filename3': '/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/temp_data/fixed_width_data.csv'
                   },
    )

    task_transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={'filename': '/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/temp_data/extracted_data.csv',},
    )

    task_unzip >> [task_extract_csv, task_extract_tsv, task_extract_txt] >> task_consolidate_data >> task_transform_data
