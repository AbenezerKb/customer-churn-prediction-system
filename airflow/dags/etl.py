import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
from pathlib import Path
from pymongo import MongoClient
import yaml
import pendulum
from dotenv import load_dotenv

load_dotenv()


DAG_FOLDER = Path(__file__).resolve().parent
CONFIG_FILE = DAG_FOLDER / "config" / "config.yml"
with open(CONFIG_FILE, "r") as f:
    cfg = yaml.safe_load(f)

def parse_start_date(date_str: str, tz: str) -> pendulum.DateTime:
    dt = pendulum.parse(date_str, tz=tz)
    if not isinstance(dt, pendulum.DateTime):
        raise ValueError(f"Invalid date format: {date_str}")
    return dt

DATABASE_URL = ""
secret_path = Path("/run/secrets/postgres_password")
if secret_path.exists():
    DATABASE_URL = (
    f"postgresql+psycopg://"
    f"{Path('/run/secrets/postgres_user').read_text().strip()}:"
    f"{secret_path.read_text().strip()}@"
    f"{os.getenv('R_POSTGRES_HOST')}:"
    f"{os.getenv('R_POSTGRES_PORT')}/"
    f"{Path('/run/secrets/postgres_db').read_text().strip()}"
)
else:
        
    DATABASE_URL = (
    f"postgresql+psycopg://"
    f"{os.getenv('R_POSTGRES_USER')}:"
    f"{os.getenv('R_POSTGRES_PASSWORD')}@"
    f"{os.getenv('R_POSTGRES_HOST')}:"
    f"{os.getenv('R_POSTGRES_PORT')}/"
    f"{os.getenv('R_POSTGRES_DB')}"
)    

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

postgres_conn_id=f"postgresql+psycopg2://{os.getenv('R_POSTGRES_USER')}:{os.getenv('R_POSTGRES_PASSWORD')}@{os.getenv('R_POSTGRES_HOST')}:{os.getenv('R_POSTGRES_PORT')}/{os.getenv('R_POSTGRES_DB')}"
def etl_process(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres_telecom_db") 
    conn = pg_hook.get_conn()
        
    
    query = """
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
        )
    ),
    latest_usage AS (
        SELECT 
            customer_id,
            monthly_minutes AS "MonthlyMinutes",
            overage_minutes AS "OverageMinutes",
            roaming_calls AS "RoamingCalls",
            perc_change_minutes AS "PercChangeMinutes"
        FROM usage_minutes
        WHERE (customer_id, period_date) IN (
            SELECT customer_id, MAX(period_date)
            FROM usage_minutes
            GROUP BY customer_id
        )
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
    
    df = df.fillna(value=0)
    
    records = df.to_dict(orient='records')


    mongo_host = os.getenv("MONGO_HOST")   
    mongo_port = os.getenv("MONGO_PORT")       
    mongo_conn=""
    db = ""
   
   
    if Path("/run/secrets/mongo_password").exists():        
        db = Path("/run/secrets/mongo_db").read_text().strip()              
        mongo_conn = f"mongodb://{Path('/run/secrets/mongo_user').read_text().strip()}:{Path('/run/secrets/mongo_password').read_text().strip()}@{mongo_host}:{mongo_port}/{db}?authSource=admin"
    else:               
        db =os.getenv("MONGO_INITDB_DATABASE")
        mongo_conn = f"mongodb://{os.getenv('MONGO_INITDB_ROOT_USERNAME')}:{os.getenv('MONGO_INITDB_ROOT_PASSWORD')}@{mongo_host}:{mongo_port}/{db}?authSource=admin"

    client = MongoClient(mongo_conn)
    try:
        client.admin.command('ping')
        print("MongoDB connection successful!")
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
    collection = client[db]['training_records'] 
    if records:
        collection.insert_many(records)
        print(f"Inserted {len(records)} documents into MongoDB.")
    else:
        print("No data to insert.")



etl_task = PythonOperator(
    task_id='etl_from_postgres_to_mongo',
    python_callable=etl_process,
    dag=dag,
)

etl_task