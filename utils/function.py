import pandas as pd
from time import time
from sqlalchemy import text
def fetch_data(query, connection):
    try:
        start_time = time()
        df = pd.read_sql(text(query), con=connection)
        query_duration = time() - start_time
        print(f"Query executed in {query_duration:.2f} seconds")
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        raise RuntimeError(f"Failed to execute the query: {e}")
