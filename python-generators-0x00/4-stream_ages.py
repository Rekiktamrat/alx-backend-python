import seed

def stream_user_ages():
    connection = seed.connect_to_prodev()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT age FROM user_data")
    for row in cursor:
        yield row['age']
    cursor.close()
    connection.close()

def average_age():
    total, count = 0, 0
    for age in stream_user_ages():
        total += age
        count += 1
    print(f"Average age of users: {total / count:.2f}")

if __name__ == "__main__":
    average_age()
