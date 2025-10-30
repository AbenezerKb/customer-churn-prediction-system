import pytest
from airflow.models import DagBag

def test_dag_loading():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dagbag.process_file("etl.py")
    
    assert "telecom_etl" in dagbag.dags
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

    dag = dagbag.get_dag("telecom_etl")
    assert dag.tasks
    assert dag.task_ids == ["etl_from_postgres_to_mongo"]
    assert dag.catchup is False
    assert dag.default_args["retries"] == 3