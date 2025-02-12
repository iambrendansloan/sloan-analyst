import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv()  # Load environment variables

def get_db_connection():
    try:
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def insert_historical_data(data, ticker="META"):
    conn = get_db_connection()
    if conn is None:
        return False

    try:
        with conn.cursor() as cur:
            for _, row in data.iterrows():
                query = """
                INSERT INTO historical_data (date, open, high, low, close, volume, ticker)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO NOTHING;
                """
                values = (
                    row.name,  # The index (date)
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    ticker
                )
                cur.execute(query, values)
            conn.commit()
            print("Data inserted successfully.")
            return True
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def get_historical_data_db(period, ticker="META"):
    conn = get_db_connection()
    if conn is None:
        return None

    try:
        with conn.cursor() as cur:
            # Query based on period
            if period == "1d":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= NOW() - INTERVAL '1 day' ORDER BY date ASC;"
            elif period == "5d":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= NOW() - INTERVAL '5 days' ORDER BY date ASC;"
            elif period == "1mo":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= NOW() - INTERVAL '1 month' ORDER BY date ASC;"
            elif period == "3mo":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= NOW() - INTERVAL '3 months' ORDER BY date ASC;"
            elif period == "6mo":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= NOW() - INTERVAL '6 months' ORDER BY date ASC;"
            elif period == "1y":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= NOW() - INTERVAL '1 year' ORDER BY date ASC;"
            elif period == "2y":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= NOW() - INTERVAL '2 years' ORDER BY date ASC;"
            elif period == "5y":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= NOW() - INTERVAL '5 years' ORDER BY date ASC;"
            elif period == "10y":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= NOW() - INTERVAL '10 years' ORDER BY date ASC;"
            elif period == "ytd":
                query = "SELECT * FROM historical_data WHERE ticker = %s AND date >= DATE_TRUNC('year', NOW()) ORDER BY date ASC;"
            elif period == "max":
                query = "SELECT * FROM historical_data WHERE ticker = %s ORDER BY date ASC;"
            else:
                return None  # Invalid

            cur.execute(query, (ticker,))
            data = cur.fetchall()

            # Convert to DataFrame
            if data:
                df = pd.DataFrame(data, columns=['id', 'date', 'open', 'high', 'low', 'close', 'volume', 'ticker'])
                df.set_index('date', inplace=True)
                df.sort_index(inplace=True)
                return df
            else:
                return None

    except psycopg2.Error as e:
        print(f"Error fetching data: {e}")
        return None
    finally:
        if conn:
            conn.close()
