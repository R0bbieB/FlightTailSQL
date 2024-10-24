import sqlite3
from datetime import datetime, timedelta
import time
import psutil
import os
import csv

# SQLite database file
DB_FILE = "flight_data.db"


def create_database():
    """
    Creates the SQLite database and table for flight data.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS flights (
        id INTEGER PRIMARY KEY,
        tail_number TEXT,
        origin TEXT,
        destination TEXT,
        date TEXT,
        dep_delay REAL,
        arr_delay REAL,
        dep_time INTEGER
    )
    ''')
    conn.commit()
    conn.close()


create_database()
