#!/usr/bin/python3
import mysql.connector
from mysql.connector import Error

def stream_users_in_batches(batch_size):
    """
    Generator that fetches rows in batches from user_data table.
    """
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='RT9300rt',  # 🔁 Replace with your MySQL root password
            database='alx_prodev'
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute("SELECT COUNT(*) AS total FROM user_data")
            total_rows = cursor.fetchone()['total']

            offset = 0
            while offset < total_rows:
                cursor.execute(
                    f"SELECT * FROM user_data LIMIT {batch_size} OFFSET {offset}"
                )
                batch = cursor.fetchall()
                if not batch:
                    break
                yield batch
                offset += batch_size

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def batch_processing(batch_size):
    """
    Processes each batch and filters users with age > 25.
    """
    for batch in stream_users_in_batches(batch_size):
        for user in batch:
            if user['age'] > 25:
                print(user)
