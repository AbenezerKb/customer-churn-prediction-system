import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, ANY
from dags.etl import etl_process, parse_start_date
import pendulum

SAMPLE_POSTGRES_DATA = [
    {
        "MonthlyRevenue": 50.0, "MonthlyMinutes": 300.0, "TotalRecurringCharge": 45.0,
        "DirectorAssistedCalls": 2.0, "OverageMinutes": 10.0, "RoamingCalls": 5.0,
        "PercChangeMinutes": 0.05, "PercChangeRevenues": -0.02, "DroppedCalls": 3,
        "BlockedCalls": 1, "UnansweredCalls": 4, "CustomerCareCalls": 2,
        "ThreewayCalls": 1, "ReceivedCalls": 120, "OutboundCalls": 80,
        "InboundCalls": 40, "PeakCallsInOut": 100, "OffPeakCallsInOut": 100,
        "DroppedBlockedCalls": 4, "CallForwardingCalls": 0, "CallWaitingCalls": 5,
        "MonthsInService": 24, "UniqueSubs": 1, "ActiveSubs": 1, "ServiceArea": "NY",
        "Handsets": 1, "HandsetModels": "iPhone12", "CurrentEquipmentDays": 180,
        "AgeHH1": 45, "AgeHH2": 42, "ChildrenInHH": True, "HandsetRefurbished": False,
        "HandsetWebCapable": True, "TruckOwner": False, "RVOwner": False,
        "Homeownership": "Known", "BuysViaMailOrder": True, "RespondsToMailOffers": True,
        "OptOutMailings": False, "NonUSTravel": False, "OwnsComputer": True,
        "HasCreditCard": True, "RetentionCalls": 0, "RetentionOffersAccepted": 0,
        "NewCellphoneUser": False, "NotNewCellphoneUser": True,
        "ReferralsMadeBySubscriber": 1, "IncomeGroup": 5, "OwnsMotorcycle": False,
        "AdjustmentsToCreditRating": 0, "HandsetPrice": 599.0,
        "MadeCallToRetentionTeam": False, "CreditRating": "A", "PrizmCode": "U1",
        "Occupation": "Professional", "MaritalStatus": "M"
    }
]

@pytest.fixture
def mock_pg_hook():
    hook = MagicMock()
    conn = MagicMock()
    hook.get_conn.return_value = conn
    return hook

@pytest.fixture
def mock_mongo_client():
    client = MagicMock()
    db = MagicMock()
    collection = MagicMock()
    client.__getitem__.return_value = db
    db.__getitem__.return_value = collection
    return client

def test_parse_start_date():
    dt = parse_start_date("2025-01-01T00:00:00+00:00", "UTC")
    assert isinstance(dt, pendulum.DateTime)
    assert dt.year == 2025

    with pytest.raises(ValueError):
        parse_start_date("invalid", "UTC")

@patch("dags.etl.PostgresHook")
@patch("dags.etl.MongoClient")
@patch("dags.etl.pd.read_sql_query")
@patch("dags.etl.Path")
def test_etl_process_success(
    mock_path, mock_read_sql, mock_mongo_client, mock_pg_hook,
    mock_env, mock_secrets, mock_pg_hook_instance, mock_mongo_client_instance
):
    mock_pg_hook.return_value = mock_pg_hook_instance
    mock_mongo_client.return_value = mock_mongo_client_instance

    df = pd.DataFrame(SAMPLE_POSTGRES_DATA)
    df = df.fillna(0)
    mock_read_sql.return_value = df

    mock_path.return_value.exists.return_value = True
    mock_path.return_value.read_text.side_effect = lambda: "mocked"

    etl_process()

    mock_read_sql.assert_called_once()
    assert "SELECT" in mock_read_sql.call_args[0][0]

    mock_mongo_client_instance.admin.command.assert_called_with('ping')
    collection = mock_mongo_client_instance["telecom"]["training_records"]
    collection.insert_many.assert_called_once()
    inserted_docs = collection.insert_many.call_args[0][0]
    assert len(inserted_docs) == 1
    assert inserted_docs[0]["MonthlyRevenue"] == 50.0
    assert inserted_docs[0]["NotNewCellphoneUser"] is True

@patch("dags.etl.PostgresHook")
@patch("dags.etl.MongoClient")
@patch("dags.etl.pd.read_sql_query")
def test_etl_process_no_data(mock_read_sql, mock_mongo_client, mock_pg_hook, mock_env):
    mock_read_sql.return_value = pd.DataFrame()  # Empty
    mock_pg_hook.return_value.get_conn.return_value = MagicMock()

    etl_process()

    # Should not call insert_many
    mock_mongo_client.return_value.__getitem__.return_value.__getitem__.return_value.insert_many.assert_not_called()