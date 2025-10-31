#!/usr/bin/python3
"""
seed_db.py - Setup script for ALX_prodev database and user_data table.
Creates database, table, and populates it with data from user_data.csv
"""

import mysql.connector
from mysql.connector import Error
import csv
import uuid
import os

# --- CONFIGURATION ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "RT9300rt"   # ⚠️ Replace with your MySQL root password if different
DATABASE_NAME = "ALX_prodev"
CSV_FILE = "user_data.csv"
# ---------------------


def connect(database=None):
    """
    Connect to MySQL server or a specific database.
    If no database is provided, connects to the server only.
    """
    try:
        if database:
            connection = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=database
            )
        else:
            connection = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD
            )

        if connection.is_connected():
            print(f"✅ Connected to MySQL {'server' if not database else database} successfully!")
            return connection
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
    return None


def create_database(connection):
    """Create the ALX_prodev database if it doesn't exist."""
    if not connection:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};")
        print(f"📘 Database '{DATABASE_NAME}' created or already exists.")
        cursor.close()
    except Error as e:
        print(f"❌ Error creating database: {e}")


def create_table(connection):
    """Create the user_data table if it doesn't exist."""
    if not connection:
        return
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_data (
                user_id CHAR(36) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                age INT NOT NULL
            );
        """)
        print("🧱 Table 'user_data' created or already exists.")
        cursor.close()
    except Error as e:
        print(f"❌ Error creating table: {e}")


def insert_data(connection, csv_file):
    """Insert data from user_data.csv into user_data table."""
    if not connection:
        return
    try:
        if not os.path.exists(csv_file):
            print(f"⚠️ File '{csv_file}' not found. Skipping data insertion.")
            return

        cursor = connection.cursor()
        with open(csv_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = str(uuid.uuid4())
                name = row['name']
                email = row['email']
                age = int(row['age'])

                query = """
                    INSERT INTO user_data (user_id, name, email, age)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE name=VALUES(name);
                """
                cursor.execute(query, (user_id, name, email, age))

        connection.commit()
        print("✅ Data inserted successfully!")
        cursor.close()
    except Error as e:
        print(f"❌ Error inserting data: {e}")
    except ValueError:
        print(f"⚠️ Invalid age value in CSV file.")
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")


def connect_to_prodev():
    """Helper function for generator scripts to connect directly to ALX_prodev."""
    return connect(DATABASE_NAME)


def main():
    """Main function to create database, table, and insert data."""
    # Step 1: Connect to MySQL server
    server_conn = connect()
    if not server_conn:
        print("❌ Could not connect to MySQL server. Exiting.")
        return

    # Step 2: Create the database
    create_database(server_conn)
    server_conn.close()

    # Step 3: Connect to the ALX_prodev database
    db_conn = connect(DATABASE_NAME)
    if not db_conn:
        print(f"❌ Could not connect to '{DATABASE_NAME}'. Exiting.")
        return

    # Step 4: Create table and insert data
    create_table(db_conn)
    insert_data(db_conn, CSV_FILE)

    # Step 5: Close the connection
    db_conn.close()
    print("✅ Setup complete. Database connection closed.")


if __name__ == "__main__":
    main()
