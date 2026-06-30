import os
import psycopg2
from dotenv import load_dotenv

# Load variables from the local .env file
load_dotenv()

def test_db():
    try:
        connection = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
        )

        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print("\n[SUCCESS] Secure connection verified!")
        print(f"[INFO] PostGIS Database Engine Version: {db_version}\n")

        cursor.close()
        connection.close()

    except Exception as error:
        print(f"\n[FAILURE] Connection failed. Error details: {error}\n")

if __name__ == "__main__":
    test_db()
