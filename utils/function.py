import pandas as pd
from time import time
from sqlalchemy import text
import streamlit as st

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
    
def remove_data(result, status_code_col='STATUS CODE', remark_col='REMARK'):
    try:
        result = result[
            ~result[remark_col].isna() &
            (result[remark_col].str.strip() != "") &
            ~result[remark_col].str.contains(
                r"Updates when case reassign to another collector|New Contact Details Added|Broken Promise|New Assignment - OS|System Auto Update Remarks For PD", 
                case=False, 
                na=False
            ) &
            ~result[status_code_col].isna() &
            (result[status_code_col].str.strip() != "") &
            ~result[status_code_col].str.lower().isin(['new', 'ptp', 'none']) &
            ~result[status_code_col].str.contains(r"ABORT|REACTIVE|PULLOUT|PULL OUT|HOLD EFFORT|LOCKED", regex=True, case=False, na=False)
        ]
        
        return result
    except Exception as e:
        st.error(f"Error in remove_data: {e}")
        raise
