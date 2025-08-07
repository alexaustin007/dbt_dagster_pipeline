from dagster import resource
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
from typing import Optional

@resource
def mysql_resource(context):
    """MySQL database resource with connection pooling and error handling."""
    
    # Get configuration from environment variables
    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME", "retail_analytics")
    
    # URL-encode the password to handle special characters like @
    encoded_password = quote_plus(db_password)
    
    connection_string = f"mysql+pymysql://{db_user}:{encoded_password}@{db_host}/{db_name}"
    
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={
            "connect_timeout": 60,
            "read_timeout": 60,
            "write_timeout": 60
        }
    )
    
    return engine 