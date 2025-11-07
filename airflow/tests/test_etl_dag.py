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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))



class TestETLDAG:
    
    @pytest.fixture
    def mock_config(self):
        return {
            "owner": "data_team",
            "depends_on_past": False,
            "start_date": "2024-01-01",
            "timezone": "UTC",
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": 5,
            "dag_id": "telecom_etl_dag",
            "description": "ETL from Postgres to Azure Blob",
            "schedule": "@daily",
            "catchup": False
        }
    
    @pytest.fixture
    def mock_env_vars(self):
        
        env_vars = {
            "R_POSTGRES_USER": "test_user",
            "R_POSTGRES_PASSWORD": "test_pass",
            "R_POSTGRES_HOST": "localhost",
            "R_POSTGRES_PORT": "5432",
            "R_POSTGRES_DB": "test_db",           
        }
        with patch.dict(os.environ, env_vars):
            yield env_vars
    
    @pytest.fixture
    def sample_dataframe(self):
        
        data = {
            "MonthlyRevenue": [100.0, 200.0],
            "MonthlyMinutes": [500, 600],
            "TotalRecurringCharge": [50.0, 75.0],
            "DirectorAssistedCalls": [2, 3],
            "OverageMinutes": [10, 20],
            "RoamingCalls": [5, 8],
            "PercChangeMinutes": [0.1, 0.15],
            "PercChangeRevenues": [0.05, 0.08],
            "DroppedCalls": [1, 2],
            "BlockedCalls": [0, 1],
            "UnansweredCalls": [3, 4],
            "CustomerCareCalls": [2, 1],
            "ThreewayCalls": [0, 1],
            "ReceivedCalls": [100, 120],
            "OutboundCalls": [80, 90],
            "InboundCalls": [50, 60],
            "PeakCallsInOut": [70, 80],
            "OffPeakCallsInOut": [60, 70],
            "DroppedBlockedCalls": [1, 3],
            "CallForwardingCalls": [5, 6],
            "CallWaitingCalls": [3, 4],
            "updated_at": "2024-01-01"
        }
        return pd.DataFrame(data)
    
    def test_parse_start_date_valid(self):
        
        from dags.etl import parse_start_date
        
        result = parse_start_date("2024-01-01", "UTC")
        assert isinstance(result, pendulum.DateTime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1
    
    def test_parse_start_date_invalid(self):
        
        from dags.etl import parse_start_date
        
        with pytest.raises(ValueError):
            parse_start_date("invalid-date", "UTC")        
    
    @patch('dags.etl.BlobServiceClient')
    @patch('dags.etl.PostgresHook')
    @patch('pandas.read_sql_query')
    def test_etl_process_success(self, mock_read_sql, mock_pg_hook, mock_blob_service, sample_dataframe):
        from dags.etl import etl_process
        import pandas as pd
        from pathlib import Path

        mock_conn = Mock()
        mock_pg_hook.return_value.get_conn.return_value = mock_conn
        mock_read_sql.return_value = sample_dataframe

        mock_blob_client_instance = MagicMock()
        mock_blob_service.return_value = mock_blob_client_instance
        mock_blob_client_instance.get_container_client.return_value = MagicMock()
        
        with (
        patch.object(Path, "exists", return_value=True),
        patch.object(Path, "read_text", return_value="fake-azure-conn-string")
    ):
            etl_process()
               
    
    @patch('builtins.open', new_callable=mock_open, read_data=yaml.dump({
        "owner": "test_owner",
        "dag_id": "test_dag",
        "start_date": "2024-01-01",
        "timezone": "UTC",
        "schedule": "@daily",
        "catchup": False,
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": 5,
        "description": "Test DAG"
    }))
    def test_config_loading(self, mock_file, mock_env_vars):
        
        import importlib
        import dags.etl
        
        with patch('pathlib.Path.exists', return_value=False):
            importlib.reload(dags.etl)
            
            assert dags.etl.cfg["owner"] == "test_owner"
            assert dags.etl.cfg["dag_id"] == "test_dag"
    
    def test_sql_query_structure(self):
        
        from dags.etl import etl_process
        

        import inspect
        source = inspect.getsource(etl_process)
        
        assert "WITH latest_billing AS" in source
        assert "latest_usage AS" in source
        assert "latest_calls AS" in source
        assert "latest_device AS" in source
        assert "LEFT JOIN demographics" in source
        assert "LEFT JOIN latest_device" in source
        assert "LEFT JOIN latest_billing" in source
        assert "LEFT JOIN latest_usage" in source
        assert "LEFT JOIN latest_calls" in source        

if __name__ == "__main__":
    pytest.main([__file__, "-v"])