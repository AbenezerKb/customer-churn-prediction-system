import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

SQL_DIR = "data"

print(f"postgres+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")
def migrate_tables():
    conn = None  
    cur = None  
    
    try:
        conn = psycopg2.connect(
            # dbname=os.getenv('POSTGRES_DB'),
            # user=os.getenv('POSTGRES_USER'),
            # password=os.getenv('POSTGRES_PASSWORD'),
            # host=os.getenv('POSTGRES_HOST'),
            # port=os.getenv('POSTGRES_PORT')
            dbname="telecom_db",
user="postgres",
password="7184e605-4a5a-4b23-be7d-50918e9a245a",
host="postgres",
port="5432"
        )
        cur = conn.cursor()
        print(f"Successfully connected to PostgreSQL at {os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")
        
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
            print(f"--- Executed {file_name} successfully.")
        
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
