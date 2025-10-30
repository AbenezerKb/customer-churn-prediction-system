import pytest
from pathlib import Path
import os
from unittest.mock import MagicMock, patch

import pytest
from unittest.mock import patch


@pytest.fixture
def mock_env():   
    env_vars = {
        "A_POSTGRES_USER": "airflow_user",
        "A_POSTGRES_PASSWORD": "7184e605-4a5a-4b23-be7d-50918e9a245a",
        "A_POSTGRES_DB": "airflow_db",
        "A_POSTGRES_HOST": "postgres_db",
        "A_POSTGRES_PORT": "5432",

        "R_POSTGRES_USER": "airflow",
        "R_POSTGRES_PASSWORD": "7184e605-4a5a-4b23-be7d-50918e9a245a",
        "R_POSTGRES_DB": "raw_db",
        "R_POSTGRES_HOST": "postgres_raw",
        "R_POSTGRES_PORT": "5433",

        "MONGO_INITDB_DATABASE": "airflow",
        "MONGO_INITDB_ROOT_USERNAME": "airflow",
        "MONGO_INITDB_ROOT_PASSWORD": "7184e605-4a5a-4b23-be7d-50918e9a245a",
        "MONGO_HOST": "mongodb",
        "MONGO_PORT": "27017",

        "AIRFLOW_USERNAME": "admin",
        "AIRFLOW_FIRSTNAME": "John",
        "AIRFLOW_LASTNAME": "Doe",
        "AIRFLOW_ROLE": "Admin",
        "AIRFLOW_EMAIL": "john@doe.com",
        "AIRFLOW_PASSWORD": "123",
    }

    with patch.dict("os.environ", env_vars):
        yield env_vars