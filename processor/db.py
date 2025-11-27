from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def connection():
    try:
        # connect to the database
        conn = psycopg2.connect(
            host = DB_HOST,
            dbname = DB_NAME,
            user = DB_USER,
            port = DB_PORT,
            password = DB_PASSWORD
        )
        print("Conenection sucessful")
        return conn
    except Exception as e:
        print("An Error occured while connecting to the database: ", e)

if __name__ == "__main__":
    connection()
