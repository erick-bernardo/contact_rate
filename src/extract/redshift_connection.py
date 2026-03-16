import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_redshift_connection():

    conn = psycopg2.connect(
        dbname=os.getenv("REDSHIFT_DATABASE"),
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        connect_timeout=300,
        sslmode="require",
        keepalives=1,
        keepalives_idle=60,
        keepalives_interval=10,
        keepalives_count=5
    )

    return conn