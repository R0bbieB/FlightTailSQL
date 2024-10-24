import sqlite3
from datetime import datetime, timedelta
import time
import psutil
import os
import csv

# SQLite database file
DB_FILE = "flight_data.db"

def get_all_tail_numbers(conn):
    """
    Retrieves all unique tail numbers from the database.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT tail_number FROM flights")
    return [row[0] for row in cursor.fetchall()]

def track_cumulative_delay(conn, tail_number, start_date, end_date):
    """
    Tracks and calculates cumulative delay for a specific aircraft over a given date range.
    """
    cursor = conn.cursor()
    query = '''
    SELECT origin, destination, date, dep_delay, arr_delay
    FROM flights
    WHERE tail_number = ? AND date >= ? AND date <= ?
    ORDER BY date, dep_time
    '''

    cursor.execute(query, (tail_number, start_date, end_date))

    cumulative_delay = 0
    flights_count = 0
    for row in cursor.fetchall():
        origin, destination, date, dep_delay, arr_delay = row
        flight_delay = max(dep_delay or 0, arr_delay or 0)  # Handle None values
        cumulative_delay += flight_delay
        flights_count += 1

    return flights_count, cumulative_delay

def main(start_date):
    """
    Main function to initiate the delay tracking process for all tail numbers.
    """
    try:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_obj = start_date_obj + timedelta(days=364)
        end_date = end_date_obj.strftime("%Y-%m-%d")

        print(f"Tracking flights for all tail numbers from {start_date} to {end_date}")

        start_time = time.time()

        conn = sqlite3.connect(DB_FILE)
        tail_numbers = get_all_tail_numbers(conn)

        results = []
        for tail_number in tail_numbers:
            flights_count, cumulative_delay = track_cumulative_delay(conn, tail_number, start_date, end_date)
            if flights_count > 0:
                results.append((tail_number, flights_count, cumulative_delay))
            print(f"Processed tail number: {tail_number}")

        conn.close()

        end_time = time.time()

        # Sort results by cumulative delay (descending order)
        results.sort(key=lambda x: x[2], reverse=True)

        # Print results
        print("\nResults (sorted by cumulative delay):")
        for tail_number, flights_count, cumulative_delay in results:
            print(f"Tail Number: {tail_number}, Flights: {flights_count}, Cumulative Delay: {cumulative_delay:.2f} minutes")

        # Save results to CSV
        csv_filename = f"delay_results_{start_date}_to_{end_date}.csv"
        with open(csv_filename, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Tail Number', 'Flights Count', 'Cumulative Delay (minutes)'])
            csvwriter.writerows(results)

        print(f"\nResults saved to {csv_filename}")

        print(f"\nPerformance Metrics:")
        print(f"Execution time: {end_time - start_time:.2f} seconds")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    start_date = input("Enter the start date (YYYY-MM-DD): ")

    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
    else:
        main(start_date)