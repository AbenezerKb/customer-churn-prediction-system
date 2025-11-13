import pytest
from pathlib import Path
import os
import sys
from unittest.mock import MagicMock, patch

sys.modules['airflow'] = MagicMock()
sys.modules['airflow.models'] = MagicMock()
sys.modules['airflow.providers'] = MagicMock()
sys.modules['airflow.providers.postgres'] = MagicMock()
sys.modules['airflow.providers.postgres.hooks'] = MagicMock()
sys.modules['airflow.providers.postgres.hooks.postgres'] = MagicMock()

@pytest.fixture
def mock_env(monkeypatch):
    env_vars = {
        "R_POSTGRES_USER": "airflow",
        "R_POSTGRES_PASSWORD": "secret",
        "R_POSTGRES_DB": "raw_db",
        "R_POSTGRES_HOST": "postgres_raw",
        "R_POSTGRES_PORT": "5433",       
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
    }.get(name, {})
    return mock


@pytest.fixture
def mock_pg_hook():
    mock = MagicMock()
    mock.get_conn.return_value = MagicMock()
    return mock