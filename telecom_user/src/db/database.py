import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends
from sqlalchemy.orm import sessionmaker
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = ""
secret_path = Path("/run/secrets/postgres_password")
if secret_path.exists():
    config = secret_path.read_text().strip()
    db = config.split("\n")
    DATABASE_URL = (
    f"postgresql+psycopg://"
    f"{db[1].strip('POSTGRES_USER=')}:"
    f"{db[2].strip('POSTGRES_PASSWORD=')}@"
    f"{db[3].strip('POSTGRES_HOST=')}:"
    f"{db[4].strip('POSTGRES_PORT=')}/"
    f"{db[0].strip('POSTGRES_DB=')}"
)
else:
        
    DATABASE_URL = (
    f"postgresql+psycopg://"
    f"{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)    

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
