import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path
from datetime import datetime
import pendulum
import yaml
import os


import sys
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.providers'] = MagicMock()
sys.modules['airflow.providers.standard'] = MagicMock()
sys.modules['airflow.providers.standard.operators'] = MagicMock()
sys.modules['airflow.providers.standard.operators.python'] = MagicMock()
sys.modules['airflow.providers.postgres'] = MagicMock()
sys.modules['airflow.providers.postgres.hooks'] = MagicMock()
sys.modules['airflow.providers.postgres.hooks.postgres'] = MagicMock()
sys.modules['airflow.providers.mongo'] = MagicMock()
sys.modules['airflow.providers.mongo.hooks'] = MagicMock()
sys.modules['airflow.providers.mongo.hooks.mongo'] = MagicMock()

class TestDAGStructure:


    @patch.dict(os.environ, {
        "R_POSTGRES_USER": "user",
        "R_POSTGRES_PASSWORD": "pass",
        "R_POSTGRES_HOST": "localhost",
        "R_POSTGRES_PORT": "5432",
        "R_POSTGRES_DB": "db"
    })
    def test_dag_initialization(self):
        
        mock_config = {
            "owner": "data_team",
            "depends_on_past": False,
            "start_date": "2024-01-01",
            "timezone": "UTC",
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": 5,
            "dag_id": "telecom_etl_dag",
            "description": "ETL from Postgres to MongoDB",
            "schedule": "@daily",
            "catchup": False
        }

        with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
            with patch('pathlib.Path.exists', return_value=False):
                
                from airflow import DAG
                DAG.reset_mock()

                import importlib
                import dags.etl
                importlib.reload(dags.etl)

                
                found = False
                for call_args in DAG.call_args_list:
                    kwargs = call_args[1]
                    if (
                        kwargs.get("dag_id") == "telecom_etl_dag"
                        and kwargs["default_args"]["owner"] == "data_team"
                        and kwargs.get("schedule") == "@daily"
                        and kwargs.get("description") == "ETL from Postgres to MongoDB"
                    ):
                        found = True
                        break

                assert found, "Expected DAG with dag_id='telecom_etl_dag' and owner='data_team' not found"

    def test_default_args_structure(self):

        from dags.etl import default_args

        required_keys = [
            'owner', 'depends_on_past', 'start_date',
            'email_on_failure', 'email_on_retry', 'retries', 'retry_delay'
        ]

        for key in required_keys:
            assert key in default_args



if __name__ == "__main__":
    pytest.main([__file__, "-v"])