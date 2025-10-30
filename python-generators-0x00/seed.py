#!/usr/bin/python3
"""
seed_db.py - Setup script for ALX_prodev database and user_data table.
"""

import mysql.connector
from mysql.connector import Error
import csv
import uuid
import os 
# It's better to use environment variables for sensitive info, 
# but for this script, we'll use a placeholder variable.

# --- CONFIGURATION ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "root"  # Use a secure password or environment variable in production
DATABASE_NAME = "ALX_prodev"
CSV_FILE = "user_data.csv"
# ---------------------


def connect(database=None):
    """
    Connect to MySQL server or a specific database.
    If database is None, connects without selecting a database (for creation).
    """
    try:
        connection = mysql.connector.connect(
            host="localhost",
    user="root",
    password="RT9300rt",
    database="world"
if database else None
        )
        if connection.is_connected():
            print(f"Connected to MySQL {'server' if not database else database} successfully!")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    return None


def create_database(connection):
    """Create the ALX_prodev database if it doesn't exist."""
    if not connection:
        return
        
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};")
        print(f"Database {DATABASE_NAME} created or already exists.")
        cursor.close()
    except Error as e:
        print(f"Error creating database: {e}")


def create_table(connection):
    """Create user_data table if it doesn't exist."""
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
        # Added UNIQUE constraint to email for better database integrity
        print("Table user_data created successfully")
        cursor.close()
    except Error as e:
        print(f"Error creating table: {e}")


def insert_data(connection, csv_file):
    """Insert data from user_data.csv into the user_data table."""
    if not connection:
        return

    try:
        cursor = connection.cursor()
        
        # Check if CSV file exists
        if not os.path.exists(csv_file):
            print(f"File {csv_file} not found. Skipping data insertion.")
            return

        with open(csv_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                user_id = str(uuid.uuid4())
                name = row['name']
                email = row['email']
                age = row['age']

                # The SELECT query is now implicitly handled by the UNIQUE constraint,
                # but an INSERT IGNORE or ON DUPLICATE KEY UPDATE is cleaner.
                # Since we want to avoid duplicates and the table now has UNIQUE email:
                
                insert_query = """
                    INSERT INTO user_data (user_id, name, email, age) 
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE name=name; -- Simple way to avoid error on duplicate email
                """
                
                # NOTE: We use INT for age, as DECIMAL is better suited for floating-point numbers.
                cursor.execute(
                    insert_query,
                    (user_id, name, email, int(age)) 
                )
        
        connection.commit()
        print("Data inserted successfully.")
        cursor.close()
    except Error as e:
        print(f"Error inserting data: {e}")
    except ValueError:
        print(f"Error: Age value '{age}' in CSV is not a valid integer.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    """Main execution function to set up the database and seed data."""
    
    # 1. Connect to MySQL server (without a database specified)
    server_conn = connect()
    if not server_conn:
        print("Exiting script due to connection failure.")
        return

    # 2. Create the target database
    create_database(server_conn)
    server_conn.close()

    # 3. Connect directly to the newly created database
    db_conn = connect(database=DATABASE_NAME)
    if not db_conn:
        print(f"Exiting script. Could not connect to {DATABASE_NAME}.")
        return

    # 4. Create the table and insert data
    create_table(db_conn)
    insert_data(db_conn, CSV_FILE)
    
    # 5. Close the database connection
    db_conn.close()
    print("Database connection closed. Setup complete.")

if __name__ == "__main__":
    main()