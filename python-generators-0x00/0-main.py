#!/usr/bin/python3

# Imports the 'seed_db.py' file (or the file you named 'seed.py')
seed = __import__('seed') 

# 1. Connect to MySQL server to create the database (no database argument)
connection = seed.connect() 
if connection:
    seed.create_database(connection)
    connection.close()
    print("Initial connection successful and database creation attempted.")

    # 2. Connect directly to the newly created database
    # Pass the database name to the connect function
    connection = seed.connect(database='ALX_prodev') 

    if connection:
        seed.create_table(connection)
        seed.insert_data(connection, 'user_data.csv')
        
        # --- Verification Logic ---
        cursor = connection.cursor()
        
        # Check if database is present (Note: The connection already proves this)
        cursor.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'ALX_prodev';")
        result = cursor.fetchone()
        if result:
            print(f"Database ALX_prodev is present ")
            
        # Select and print data
        cursor.execute(f"SELECT * FROM user_data LIMIT 5;")
        rows = cursor.fetchall()
        print("First 5 rows of data:")
        print(rows)
        
        cursor.close()
        connection.close() # Ensure final connection is closed