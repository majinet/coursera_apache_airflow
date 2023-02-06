from os.path import exists
import datetime

import pendulum
import pytest

from airflow import DAG
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from dags.finalassignment import etl_workflow

DATA_INTERVAL_START = pendulum.datetime(2022, 2, 8, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_TASK_ID_1 = "unzip_data"


@pytest.fixture()
def dag():
    dag = etl_workflow.dag

    return dag


def test_task_unzip_data(dag):
    print(f"dag: {dag}")

    dagrun = dag.create_dagrun(
        run_id="1",
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )
    ti = dagrun.get_task_instance(task_id=TEST_TASK_ID_1)

    print(f"ti: {type(ti)}, ti: {ti}")
    ti.task = dag.get_task(task_id=TEST_TASK_ID_1)
    ti.run(ignore_ti_state=True)

    assert exists("/mnt/d/learning/ibm/dataops/build_airflow_workflow/staging/fileformats.txt")
    # Assert something related to tasks results.