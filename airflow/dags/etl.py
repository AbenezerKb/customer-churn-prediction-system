import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

import pandas as pd
from pathlib import Path
import yaml
import time
import logging
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import pendulum
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DAG_FOLDER = Path(__file__).resolve().parent
CONFIG_FILE = DAG_FOLDER / "config" / "config.yml"

if not os.path.exists(CONFIG_FILE):
    logging.info("file not found")
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    CONFIG_FILE = PROJECT_ROOT / "config" / "config.yml"

with open(CONFIG_FILE, "r") as f:
    cfg = yaml.safe_load(f)

def parse_start_date(date_str: str, tz: str) -> pendulum.DateTime:
    dt = pendulum.parse(date_str, tz=tz)
    if not isinstance(dt, pendulum.DateTime):
        raise ValueError(f"Invalid date format: {date_str}")
    return dt

DATABASE_URL = ""
secret_path = Path("/run/secrets/r_postgres_password")
if secret_path.exists():  
    secret_path = secret_path.read_text().strip()    
    postgres_user = Path('/run/secrets/r_postgres_user').read_text().strip()
    postgres_db = Path('/run/secrets/r_postgres_db').read_text().strip()
    DATABASE_URL = (
    f"postgresql+psycopg://"
    f"{postgres_user}:"
    f"{secret_path}@"
    f"{os.getenv('R_POSTGRES_HOST')}:"
    f"{os.getenv('R_POSTGRES_PORT')}/"
    f"{Path('/run/secrets/r_postgres_db').read_text().strip()}"
)
    postgres_conn_id=f"postgresql+psycopg2://{postgres_user}:{secret_path}@{os.getenv('R_POSTGRES_HOST')}:{os.getenv('R_POSTGRES_PORT')}/{postgres_db}"
else:   
    DATABASE_URL = (
    f"postgresql+psycopg://"
    f"{os.getenv('R_POSTGRES_USER')}:"
    f"{os.getenv('R_POSTGRES_PASSWORD')}@"
    f"{os.getenv('R_POSTGRES_HOST')}:"
    f"{os.getenv('R_POSTGRES_PORT')}/"
    f"{os.getenv('R_POSTGRES_DB')}"
)    
    postgres_conn_id=f"postgresql+psycopg2://{os.getenv('R_POSTGRES_USER')}:{os.getenv('R_POSTGRES_PASSWORD')}@{os.getenv('R_POSTGRES_HOST')}:{os.getenv('R_POSTGRES_PORT')}/{os.getenv('R_POSTGRES_DB')}"

default_args = {
    'owner': cfg["owner"],
    'depends_on_past': cfg["depends_on_past"],
    'start_date': parse_start_date(cfg["start_date"], cfg["timezone"]),
    'email_on_failure': cfg["email_on_failure"],
    'email_on_retry': cfg["email_on_retry"],
    'retries': cfg["retries"],
    'retry_delay': cfg["retry_delay"],     
}

dag = DAG(
    dag_id=cfg["dag_id"],    
    default_args=default_args,
    description=cfg["description"],
    schedule=cfg["schedule"],    
    catchup=cfg["catchup"],
)

def etl_process(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres_telecom_db") 
    conn = pg_hook.get_conn()
        
    last_loaded_at = Variable.get("last_loaded_at", default_var="1970-01-01T00:00:00")
    query = f"""
    WITH latest_billing AS (
        SELECT 
            customer_id,
            monthly_revenue AS "MonthlyRevenue",
            total_recurring_charge AS "TotalRecurringCharge",
            perc_change_revenues AS "PercChangeRevenues"
        FROM billing
        WHERE (customer_id, period_date) IN (
            SELECT customer_id, MAX(period_date)
            FROM billing
            GROUP BY customer_id
        ) AND updated_at > '{last_loaded_at}'
        ORDER BY updated_at
    ),
    latest_usage AS (
        SELECT 
            customer_id,
            monthly_minutes AS "MonthlyMinutes",
            overage_minutes AS "OverageMinutes",
            roaming_calls AS "RoamingCalls",
            perc_change_minutes AS "PercChangeMinutes",
            updated_at
        FROM usage_minutes
        WHERE (customer_id, period_date) IN (
            SELECT customer_id, MAX(period_date)
            FROM usage_minutes
            GROUP BY customer_id
        )
         AND updated_at > '{last_loaded_at}'
        ORDER BY updated_at
    ),
    latest_calls AS (
        SELECT 
            customer_id,
            dropped_calls AS "DroppedCalls",
            blocked_calls AS "BlockedCalls",
            unanswered_calls AS "UnansweredCalls",
            customer_care_calls AS "CustomerCareCalls",
            three_way_calls AS "ThreewayCalls",
            received_calls AS "ReceivedCalls",
            outbound_calls AS "OutboundCalls",
            inbound_calls AS "InboundCalls",
            peak_calls_in_out AS "PeakCallsInOut",
            off_peak_calls_in_out AS "OffPeakCallsInOut",
            dropped_blocked_calls AS "DroppedBlockedCalls",  -- Or compute as dropped_calls + blocked_calls
            call_forwarding_calls AS "CallForwardingCalls",
            call_waiting_calls AS "CallWaitingCalls",
            director_assisted_calls AS "DirectorAssistedCalls"
        FROM call_details
        WHERE (customer_id, period_date) IN (
            SELECT customer_id, MAX(period_date)
            FROM call_details
            GROUP BY customer_id
        )
         AND updated_at > '{last_loaded_at}'
        ORDER BY updated_at
    ),
    latest_device AS (
        SELECT 
            d.customer_id,
            d.handsets AS "Handsets",
            d.handset_models AS "HandsetModels",
            d.current_equipment_days AS "CurrentEquipmentDays",
            d.handset_refurbished AS "HandsetRefurbished",
            d.handset_web_capable AS "HandsetWebCapable",
            d.handset_price AS "HandsetPrice"
        FROM device d
        INNER JOIN (
            SELECT customer_id, MAX(activation_date) AS max_activation
            FROM device
            GROUP BY customer_id
        ) ld ON d.customer_id = ld.customer_id AND d.activation_date = ld.max_activation
         AND d.updated_at > '{last_loaded_at}'
        ORDER BY d.updated_at
    )
    SELECT 
        lb."MonthlyRevenue",
        lu."MonthlyMinutes",
        lb."TotalRecurringCharge",
        lc."DirectorAssistedCalls",
        lu."OverageMinutes",
        lu."RoamingCalls",
        lu."PercChangeMinutes",
        lb."PercChangeRevenues",
        lc."DroppedCalls",
        lc."BlockedCalls",
        lc."UnansweredCalls",
        lc."CustomerCareCalls",
        lc."ThreewayCalls",
        lc."ReceivedCalls",
        lc."OutboundCalls",
        lc."InboundCalls",
        lc."PeakCallsInOut",
        lc."OffPeakCallsInOut",
        lc."DroppedBlockedCalls",
        lc."CallForwardingCalls",
        lc."CallWaitingCalls",
        lu."updated_at",
        c.months_in_service AS "MonthsInService",
        c.unique_subs AS "UniqueSubs",
        c.active_subs AS "ActiveSubs",
        c.service_area AS "ServiceArea",
        ld."Handsets",
        ld."HandsetModels",
        ld."CurrentEquipmentDays",
        dem.age_hh1 AS "AgeHH1",
        dem.age_hh2 AS "AgeHH2",
        dem.children_in_hh AS "ChildrenInHH",
        ld."HandsetRefurbished",
        ld."HandsetWebCapable",
        dem.truck_owner AS "TruckOwner",
        dem.rv_owner AS "RVOwner",
        dem.homeownership AS "Homeownership",
        dem.buys_via_mail_order AS "BuysViaMailOrder",
        dem.responds_to_mail_offers AS "RespondsToMailOffers",
        dem.opt_out_mailings AS "OptOutMailings",
        dem.non_us_travel AS "NonUSTravel",
        dem.owns_computer AS "OwnsComputer",
        dem.has_credit_card AS "HasCreditCard",
        c.retention_calls AS "RetentionCalls",
        c.retention_offers_accepted AS "RetentionOffersAccepted",
        c.new_cellphone_user AS "NewCellphoneUser",
        (NOT c.new_cellphone_user) AS "NotNewCellphoneUser",  -- Derived
        c.referrals_made_by_subscriber AS "ReferralsMadeBySubscriber",
        dem.income_group AS "IncomeGroup",
        dem.owns_motorcycle AS "OwnsMotorcycle",
        c.adjustments_to_credit_rating AS "AdjustmentsToCreditRating",
        ld."HandsetPrice",
        c.made_call_to_retention_team AS "MadeCallToRetentionTeam",
        c.credit_rating AS "CreditRating",
        dem.prizm_code AS "PrizmCode",
        dem.occupation AS "Occupation",
        dem.marital_status AS "MaritalStatus"
    FROM customer c
    LEFT JOIN demographics dem ON c.id = dem.customer_id
    LEFT JOIN latest_device ld ON c.id = ld.customer_id
    LEFT JOIN latest_billing lb ON c.id = lb.customer_id
    LEFT JOIN latest_usage lu ON c.id = lu.customer_id
    LEFT JOIN latest_calls lc ON c.id = lc.customer_id;
    """
    
    df = pd.read_sql_query(query, conn)
    
    conn.close()
    
    if not df.empty:
        df = df.fillna(value=0)
        account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        azure_connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        azure_container = os.getenv("AZURE_CONTAINER_NAME")

        timestamp = int(time.time())
        blob_name = f"training_records_{timestamp}.csv"

        if Path("/run/secrets/azure_connection_string").exists():
            azure_connection_string = Path("/run/secrets/azure_connection_string").read_text().strip()
            azure_container = Path("/run/secrets/azure_container_name").read_text().strip() if Path("/run/secrets/azure_container_name").exists() else azure_container       
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        container_client = blob_service_client.get_container_client(azure_container)

        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

        new_last = df["updated_at"].max().isoformat()
        Variable.set("last_loaded_at", new_last)
        df = df.drop("updated_at",  axis=1)
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        blob_client = container_client.get_blob_client(blob_name)
        try:
            blob_client.upload_blob(csv_buffer, overwrite=True)
            logging.info(f"Uploaded {blob_name} to Azure Blob Storage container {azure_container}.")
        except Exception as e:
            logging.error(f"Azure Blob upload failed: {e}")   
    else:
        logging.info("No data to insert") 


etl_task = PythonOperator(
    task_id='etl_from_postgres_to_azure_blob',
    python_callable=etl_process,
    dag=dag,
)

etl_task