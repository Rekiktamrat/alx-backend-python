import mysql.connector

def stream_users():
    """Generator that streams rows one by one from the user_data table"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",          # change if you use a different username
            password="RT9300rt",  # replace with your real root password
            database="alx_prodev"  # replace with your real DB name
        )

        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM user_data")

        for row in cursor:
            yield row

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if connection and connection.is_connected():
            connection.close()
