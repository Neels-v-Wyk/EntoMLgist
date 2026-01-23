"""Database configuration and engine setup."""
import os
from dotenv import load_dotenv
from sqlmodel import create_engine

# Load environment variables from .env file
load_dotenv()

# Create SQLModel engine
DATABASE_URL = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
    user=os.getenv('DB_USER', 'entomlgist'),
    password=os.getenv('DB_PASSWORD', 'entomlgist_dev_password'),
    host=os.getenv('DB_HOST', 'localhost'),
    port=os.getenv('DB_PORT', '5432'),
    database=os.getenv('DB_NAME', 'entomlgist')
)

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
