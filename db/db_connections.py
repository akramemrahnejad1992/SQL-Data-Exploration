import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()  
def create_connection():
    db_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST')
    }
    return psycopg2.connect(**db_params)
