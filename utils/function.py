import pandas as pd
from time import time
from sqlalchemy import text
import streamlit as st

def get_raw_file(file, sheet_name=None, engine=None):
    try:
        if sheet_name is None:
            return pd.read_excel(file, dtype=str, engine=engine)
        else:
            return pd.read_excel(file, sheet_name=sheet_name, engine=engine, dtype=str)
    
    except Exception as e:
        st.write(e)
        st.error(f'''**ERROR:** File doesn\'t exist! Please check the file path and try again.
                 
                 File Path: {file}''')

        return pd.DataFrame([])
    
    
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

def clean_and_split_numbers(number):
    """Splits multiple numbers from a single string, removes duplicates, and keeps only valid numbers."""
    if pd.isna(number):
        return []
    
    numbers = str(number).split()
    unique_numbers = list(set(num.strip() for num in numbers if num.strip().isdigit()))
    return unique_numbers

def prioritize_phones(numbers):
    """Sort and prioritize phone numbers in PH format first, ensuring no duplicates."""
    cleaned_numbers = set()
    for num in numbers:
        cleaned_numbers.update(clean_and_split_numbers(num))

    sorted_numbers = sorted(cleaned_numbers, key=lambda x: not (x.startswith("63") or x.startswith("09")))
    return sorted_numbers[:5] + [""] * (5 - len(sorted_numbers))

def update_phone1(row):
    phone1_value = str(row["phone1"]).strip()  # Convert to string & remove whitespace
    
    # Check if phone1 is empty OR less than 8 digits (but not null)
    if not phone1_value or (phone1_value.replace(" ", "").isdigit() and len(phone1_value) < 8):
        row["phone1"] = "101011"  # Replace with "101011"
    
    return row

def fix_phone1(row):
    if row["phone1"] == "101011":  
        for col in ["phone2", "phone3", "phone4", "phone5"]:  
            if pd.notna(row[col]) and str(row[col]).strip():  
                row["phone1"] = str(row[col]).strip()  
                row[col] = ""  # Clear original value
                break  # Stop after first valid number
    return row

def format_phone_numbers(row):
    phone_cols = ["phone1", "phone2", "phone3", "phone4", "phone5"]
    
    for col in phone_cols:
        phone_value = str(row[col]).strip()  # Ensure it's a clean string
        
        # Convert "639XXXXXXXXX" → "09XXXXXXXXX"
        if phone_value.startswith("63") and len(phone_value) == 12:
            row[col] = "0" + phone_value[2:]  # Replace "63" with "0"
        
        # Convert "9XXXXXXXXX" → "09XXXXXXXXX" (if exactly 10 digits)
        elif phone_value.isnumeric() and len(phone_value) == 10 and phone_value[0] == "9":
            row[col] = "0" + phone_value  # Prepend "0"

    return row

def load_mappings(client_name, config_path):
    """Load column mappings from the sheet corresponding to the selected client."""
    try:
        df_client_mappings = pd.read_excel(config_path, sheet_name=client_name)
        mappings = list(zip(df_client_mappings["Database Column"], df_client_mappings["Mapped Column"]))
        return mappings
    except ValueError:
        st.error(f"No sheet found for {client_name} in {config_path}")
        return []

def chunk_list(lst, chunk_size):
    """Split a list into smaller chunks of given size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

    