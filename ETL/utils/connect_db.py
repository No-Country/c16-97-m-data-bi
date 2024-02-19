import os
import psycopg2
from psycopg2 import connect
from dotenv import load_dotenv
load_dotenv()

def connect_to_db() -> dict:
    return {
        "host": os.getenv('HOST'),
        "user": os.getenv('USER'),
        "password": os.getenv('PASSWORD'),
    }