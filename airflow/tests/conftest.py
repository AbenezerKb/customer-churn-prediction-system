import pytest
from pathlib import Path
import os
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_env(monkeypatch):
    env_vars = {
        "R_POSTGRES_USER": "airflow",
        "R_POSTGRES_PASSWORD": "secret",
        "R_POSTGRES_DB": "raw_db",
        "R_POSTGRES_HOST": "postgres_raw",
        "R_POSTGRES_PORT": "5433",
        "MONGO_INITDB_ROOT_USERNAME": "airflow",
        "MONGO_INITDB_ROOT_PASSWORD": "secret",
        "MONGO_HOST": "mongodb",
        "MONGO_PORT": "27017",
        "MONGO_INITDB_DATABASE": "airflow",
    }
    with pytest.monkeypatch.context() as m:
        for k, v in env_vars.items():
            m.setenv(k, v)
        yield env_vars


@pytest.fixture
def mock_secrets(mock_env):
    mock = MagicMock()
    mock.get_secret.side_effect = lambda name: {
        "postgres_raw": {
            "user": os.getenv("R_POSTGRES_USER"),
            "password": os.getenv("R_POSTGRES_PASSWORD"),
            "host": os.getenv("R_POSTGRES_HOST"),
            "port": int(os.getenv("R_POSTGRES_PORT")),
            "dbname": os.getenv("R_POSTGRES_DB"),
        },
        "mongo": {
            "uri": f"mongodb://{os.getenv('MONGO_INITDB_ROOT_USERNAME')}:{os.getenv('MONGO_INITDB_ROOT_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{os.getenv('MONGO_INITDB_DATABASE')}"
        }
    }.get(name, {})
    return mock


@pytest.fixture
def mock_pg_hook():
    mock = MagicMock()
    mock.get_conn.return_value = MagicMock()
    return mock


@pytest.fixture
def mock_mongo_client():
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    mock_db.__getitem__.return_value = mock_collection
    return mock_client