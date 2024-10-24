import sqlite3
from datetime import datetime, timedelta
import time
import psutil
import os
import csv

# SQLite database file
DB_FILE = "flight_data.db"


def import_data_from_csv(csv_file):
    """
    Imports data from the CSV file into the SQLite database.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    with open(csv_file, 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            date_parts = row['Date'].split('-')
            formatted_date = f"{date_parts[2]}-{date_parts[1]}-{date_parts[0]}"

            cursor.execute('''
            INSERT INTO flights (tail_number, origin, destination, date, dep_delay, arr_delay, dep_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['TailNum'],
                row['Origin'],
                row['Dest'],
                formatted_date,
                float(row['DepDelay']),
                float(row['ArrDelay']),
                int(row['DepTime'])
            ))

    conn.commit()
    conn.close()


import_data_from_csv("Flight_delay.csv")
