import pandas as pd

def run_queries(query, conn):
    try:
        db_data = pd.read_sql_query(query, conn)
        return db_data
    except Exception as e:
        print(f'Error reading data: {e}')
    finally:
        # Close the database connection
        conn.close()
    return None
