import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SQL_DIR = "seed"

POSTGRES_DB=os.getenv('R_POSTGRES_DB')
POSTGRES_USER=os.getenv('R_POSTGRES_USER')
POSTGRES_PASSWORD=os.getenv('R_POSTGRES_PASSWORD')
POSTGRES_HOST=os.getenv('R_POSTGRES_HOST')
POSTGRES_PORT=os.getenv('R_POSTGRES_PORT')
password = Path("/run/secrets/r_postgres_password")
if password.exists():
    POSTGRES_DB= Path('/run/secrets/r_postgres_db').read_text().strip()
    POSTGRES_USER= Path('/run/secrets/r_postgres_user').read_text().strip()
    POSTGRES_PASSWORD= Path("/run/secrets/r_postgres_password").read_text().strip() 
    POSTGRES_HOST= os.getenv('R_POSTGRES_HOST')
    POSTGRES_PORT= os.getenv('R_POSTGRES_PORT')


def migrate_tables():
    conn = None 
    cur = None  
    
    try:
       
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
        )
        cur = conn.cursor()
        print(f"Successfully connected to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}")
        
        sql_files = [
            "01_customer.sql",
            "02_demographics.sql",
            "03_device.sql",
            "04_billing.sql",
            "05_usage_minutes.sql",
            "06_call_details.sql",
        ]
        
        conn.autocommit = False
        
        for file_name in sql_files:
            file_path = os.path.join(SQL_DIR, file_name)
            print(f"Executing {file_name}...")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"SQL file not found: {file_path}")
            
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            cur.execute(sql_content)
            print(f"Executed {file_name} successfully.")
        
        conn.commit()
        print("Migration completed successfully.")
    
    except psycopg2.Error as e:
        print(f"Database error during migration: {e}")
        if conn:
            conn.rollback()
            print("Transaction was rolled back.")
    except FileNotFoundError as e:
        print(f"File error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during migration: {e}")
        if conn:
            conn.rollback()
            print("Transaction was rolled back due to unexpected error.")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    migrate_tables()
