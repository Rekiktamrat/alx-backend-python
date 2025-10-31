#!/usr/bin/env python3
"""
Memory-efficient average age calculator using generators
"""

import csv

def stream_user_ages(filename="user_data.csv"):
    """Generator that yields user ages one by one from the CSV file"""
    with open(filename, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                yield int(row["age"])
            except (ValueError, KeyError):
                continue  # Skip invalid or missing data


def calculate_average_age():
    """Calculate average age without loading the entire dataset into memory"""
    total_age = 0
    count = 0

    for age in stream_user_ages():
        total_age += age
        count += 1

    if count == 0:
        print("No user data available.")
        return

    average_age = total_age / count
    print(f"Average age of users: {average_age:.2f}")


if __name__ == "__main__":
    calculate_average_age()
