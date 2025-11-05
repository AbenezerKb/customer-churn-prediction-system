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
            "description": "ETL from Postgres to MongoDB",
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
            "MONGO_HOST": "localhost",
            "MONGO_PORT": "27017",
            "MONGO_INITDB_DATABASE": "test_mongo_db",
            "MONGO_INITDB_ROOT_USERNAME": "mongo_user",
            "MONGO_INITDB_ROOT_PASSWORD": "mongo_pass"
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
            "CallWaitingCalls": [3, 4]
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
    
    @patch('pathlib.Path.exists')
    def test_database_url_with_secrets(self, mock_exists, mock_env_vars):

        mock_exists.return_value = True
        
        with patch('pathlib.Path.read_text') as mock_read:
            mock_read.side_effect = ['db_user', 'db_pass', 'db_name']
            
            import importlib
            import dags.etl
            importlib.reload(dags.etl)
            
            expected_url = "postgresql+psycopg://db_user:db_pass@localhost:5432/db_name"
            assert dags.etl.DATABASE_URL == expected_url
    
    @patch('pathlib.Path.exists')
    def test_database_url_without_secrets(self, mock_exists, mock_env_vars):
        mock_exists.return_value = False
        
        import importlib
        import dags.etl
        importlib.reload(dags.etl)
        
        expected_url = "postgresql+psycopg://test_user:test_pass@localhost:5432/test_db"
        assert dags.etl.DATABASE_URL == expected_url
    
    @patch('dags.etl.PostgresHook')
    @patch('dags.etl.MongoClient')
    @patch('pandas.read_sql_query')
    def test_etl_process_success(self, mock_read_sql, mock_mongo_client, 
                                  mock_pg_hook, sample_dataframe, mock_env_vars):
        from dags.etl import etl_process
        
        mock_conn = Mock()
        mock_pg_hook.return_value.get_conn.return_value = mock_conn
        
        mock_read_sql.return_value = sample_dataframe
        
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        
        mock_mongo_instance = MagicMock()
        mock_mongo_instance.__getitem__.return_value = mock_db
        mock_mongo_instance.admin.command.return_value = True
        mock_mongo_client.return_value = mock_mongo_instance
        
        with patch('pathlib.Path.exists', return_value=False):
            etl_process()
        
        mock_pg_hook.assert_called_once_with(postgres_conn_id="postgres_telecom_db")
        mock_read_sql.assert_called_once()
        mock_conn.close.assert_called_once()
        mock_collection.insert_many.assert_called_once()
        
        call_args = mock_collection.insert_many.call_args[0][0]
        assert len(call_args) == 2
    
    @patch('dags.etl.PostgresHook')
    @patch('dags.etl.MongoClient')
    @patch('pandas.read_sql_query')
    def test_etl_process_fills_na_values(self, mock_read_sql, mock_mongo_client,
                                          mock_pg_hook, mock_env_vars):
        from dags.etl import etl_process
        
        df_with_na = pd.DataFrame({
            "MonthlyRevenue": [100.0, None],
            "MonthlyMinutes": [500, 600]
        })
        
        mock_conn = Mock()
        mock_pg_hook.return_value.get_conn.return_value = mock_conn
        mock_read_sql.return_value = df_with_na
        
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        
        mock_mongo_instance = MagicMock()
        mock_mongo_instance.__getitem__.return_value = mock_db
        mock_mongo_instance.admin.command.return_value = True
        mock_mongo_client.return_value = mock_mongo_instance
        
        with patch('pathlib.Path.exists', return_value=False):
            etl_process()
        
        call_args = mock_collection.insert_many.call_args[0][0]
        assert call_args[1]["MonthlyRevenue"] == 0
    
    @patch('dags.etl.PostgresHook')
    @patch('dags.etl.MongoClient')
    @patch('pandas.read_sql_query')
    def test_etl_process_empty_dataframe(self, mock_read_sql, mock_mongo_client,
                                          mock_pg_hook, mock_env_vars, capsys):

        from dags.etl import etl_process
        
        mock_conn = Mock()
        mock_pg_hook.return_value.get_conn.return_value = mock_conn
        mock_read_sql.return_value = pd.DataFrame()
        
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        
        mock_mongo_instance = MagicMock()
        mock_mongo_instance.__getitem__.return_value = mock_db
        mock_mongo_instance.admin.command.return_value = True
        mock_mongo_client.return_value = mock_mongo_instance
        
        with patch('pathlib.Path.exists', return_value=False):
            etl_process()
        
        mock_collection.insert_many.assert_not_called()
        
        captured = capsys.readouterr()
        assert "No data to insert" in captured.out
    
    @patch('dags.etl.PostgresHook')
    @patch('dags.etl.MongoClient')
    def test_etl_process_mongo_connection_failure(self, mock_mongo_client,
                                                    mock_pg_hook, mock_env_vars, capsys):
        from dags.etl import etl_process
        
        mock_conn = Mock()
        mock_pg_hook.return_value.get_conn.return_value = mock_conn
        

        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        
        mock_mongo_instance = MagicMock()
        mock_mongo_instance.__getitem__.return_value = mock_db
        mock_mongo_instance.admin.command.side_effect = Exception("Connection failed")
        mock_mongo_client.return_value = mock_mongo_instance
        
        with patch('pathlib.Path.exists', return_value=False):
            with patch('pandas.read_sql_query', return_value=pd.DataFrame()):
                etl_process()
        
        captured = capsys.readouterr()
        assert "MongoDB connection failed" in captured.out
        
        mock_collection.insert_many.assert_not_called()
    
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
        
        # Check for key CTEs and joins
        assert "WITH latest_billing AS" in source
        assert "latest_usage AS" in source
        assert "latest_calls AS" in source
        assert "latest_device AS" in source
        assert "LEFT JOIN demographics" in source
        assert "LEFT JOIN latest_device" in source
        assert "LEFT JOIN latest_billing" in source
        assert "LEFT JOIN latest_usage" in source
        assert "LEFT JOIN latest_calls" in source
    
    @patch('dags.etl.MongoClient')
    def test_mongo_connection_with_secrets(self, mock_mongo_client, mock_env_vars):
        """Test MongoDB connection string with Docker secrets"""
        from dags.etl import etl_process
        
        with patch('pathlib.Path.exists', return_value=True):
            with patch('pathlib.Path.read_text') as mock_read:
                mock_read.side_effect = ['mongo_db', 'mongo_user', 'mongo_pass']
                
                mock_pg_hook = Mock()
                mock_conn = Mock()
                mock_pg_hook.return_value.get_conn.return_value = mock_conn
                
                with patch('dags.etl.PostgresHook', return_value=mock_pg_hook):
                    with patch('pandas.read_sql_query', return_value=pd.DataFrame()):
                        # Properly mock MongoDB with subscript support using MagicMock
                        mock_collection = MagicMock()
                        mock_db = MagicMock()
                        mock_db.__getitem__.return_value = mock_collection
                        
                        mock_mongo_instance = MagicMock()
                        mock_mongo_instance.__getitem__.return_value = mock_db
                        mock_mongo_instance.admin.command.return_value = True
                        mock_mongo_client.return_value = mock_mongo_instance
                        
                        etl_process()
                
                # Verify MongoDB connection string format
                call_args = mock_mongo_client.call_args[0][0]
                assert "mongodb://mongo_user:mongo_pass@" in call_args
                assert "authSource=admin" in call_args

if __name__ == "__main__":
    pytest.main([__file__, "-v"])