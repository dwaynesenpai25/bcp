import os
import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError
from utils.db import db_engine
from utils.function import *
import json

def load_mappings(client_name,config_path):
    """Load column mappings from the sheet corresponding to the selected client."""
    try:
        df_client_mappings = pd.read_excel(config_path, sheet_name=client_name)

        # Convert to list of tuples to keep duplicates
        mappings = list(zip(df_client_mappings["Database Column"], df_client_mappings["Mapped Column"]))

        return mappings
    except ValueError:
        st.error(f"No sheet found for {client_name} in {config_path}")
        return []
def info(selected_client,config_path):
    """Fetch data for the selected client using dynamic column mappings."""
    try:
        volare = db_engine('volare')

        # Load dynamic mappings from the selected client's sheet
        mappings = load_mappings(selected_client,config_path)

        if not mappings:
            st.warning(f"No mappings found for {selected_client}.")
            return None

        # Generate the SELECT clause dynamically
        select_clause = ",\n".join([f"{db_col} AS '{mapped_col}'" for db_col, mapped_col in mappings])

        st.write(select_clause)
        sql_query = f"""
            SELECT DISTINCT
                {select_clause}
            FROM debtor
                LEFT JOIN debtor_followup ON debtor_followup.debtor_id = debtor.id
                LEFT JOIN followup ON followup.id = debtor_followup.followup_id
                LEFT JOIN `user` ON `user`.id = followup.remark_by_id
                LEFT JOIN `client` ON client.id = debtor.client_id
                LEFT JOIN debtor_address ON debtor_address.debtor_id = debtor.id
                LEFT JOIN `address` ON address.id = debtor_address.address_id
            WHERE client.id IN ('75')
            AND debtor.is_aborted <> 1
            AND debtor.is_locked <> 1
            AND followup.datetime >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
            AND YEAR(followup.datetime) = YEAR(CURRENT_DATE)
            AND MONTH(followup.datetime) = MONTH(CURRENT_DATE);

        """
        st.code = (sql_query)
        df = fetch_data(sql_query, volare)

        return df
    except SQLAlchemyError as e:
        st.error(f"Error fetching data: {e}")
        return None
    
def contact(debtor_ids):
    """Fetch current month's data from the database."""
    try:
        id_list = ', '.join(map(str, debtor_ids))
        volare = db_engine('volare')
        sql_query = f"""
            SELECT DISTINCT
                debtor.id AS 'ch_code',
                contact_number.contact_number AS 'number'
            FROM debtor
                LEFT JOIN debtor_followup ON debtor_followup.debtor_id = debtor.id
                LEFT JOIN followup ON followup.id = debtor_followup.followup_id
                LEFT JOIN `client` ON client.id = debtor.client_id
                LEFT JOIN contact_number  ON contact_number.id= followup.contact_number_id
            WHERE client.id IN ('75')
                AND debtor.is_aborted <> 1
                AND debtor.is_locked <> 1
                AND debtor.id IN ({id_list})  -- Filter by debtor IDs
                AND contact_number.contact_number <> 'NA'
                AND contact_number.contact_number IS NOT NULL
        """
        st.code = (sql_query)
        df = fetch_data(sql_query, volare)
       
        return df
    except SQLAlchemyError as e:
        st.error(f"Error fetching data: {e}")
        return None

def address(debtor_ids):
    """Fetch current month's data from the database."""
    try:
        id_list = ', '.join(map(str, debtor_ids))
        volare = db_engine('volare')
        sql_query = f"""
            SELECT DISTINCT
                debtor.id AS 'ch_code',
                address.address AS 'address'
            FROM debtor
                LEFT JOIN debtor_followup ON debtor_followup.debtor_id = debtor.id
                LEFT JOIN followup ON followup.id = debtor_followup.followup_id
                LEFT JOIN `client` ON client.id = debtor.client_id
                LEFT JOIN debtor_address ON debtor_address.debtor_id = debtor.id
                LEFT JOIN `address` ON address.id = debtor_address.address_id
            WHERE client.id IN ('75')
                AND debtor.is_aborted <> 1
                AND debtor.is_locked <> 1
                AND debtor.id IN ({id_list})  -- Filter by debtor IDs
                AND address.address <> 'NA'
                AND address.address IS NOT NULL
        """
        st.code = (sql_query)
        df = fetch_data(sql_query, volare)
       
        return df
    except SQLAlchemyError as e:
        st.error(f"Error fetching data: {e}")
        return None

def clean_and_split_numbers(number):
    """Splits multiple numbers from a single string, removes duplicates, and keeps only valid numbers."""
    if pd.isna(number):
        return []
    
    numbers = str(number).split()  # Split using spaces
    unique_numbers = list(set(num.strip() for num in numbers if num.strip().isdigit()))  # Remove duplicates
    
    return unique_numbers

def prioritize_phones(numbers):
    """Sort and prioritize phone numbers in PH format first, ensuring no duplicates."""
    cleaned_numbers = set()  # Use a set to store unique numbers
    for num in numbers:
        cleaned_numbers.update(clean_and_split_numbers(num))  # Add unique numbers

    # Convert back to list and sort, prioritizing PH numbers
    sorted_numbers = sorted(cleaned_numbers, key=lambda x: not (x.startswith("63") or x.startswith("09")))

    # Fill up to 5 slots with empty strings if fewer than 5 numbers exist
    return sorted_numbers[:5] + [""] * (5 - len(sorted_numbers))


# Streamlit UI
st.title("📊 Database Query & Export Tool")
config_path="/home/spm/Documents/BCP/config/config.xlsx"
xls = pd.ExcelFile(config_path)
available_clients = xls.sheet_names 

selected_client = st.selectbox("Select a Client", available_clients)

if selected_client:
    create_btn = st.button("Fetch Data")
else:
    st.warning("No clients available in the config file.")

if create_btn:
    df = info(selected_client,config_path)
    
    if df is not None:
        debtor_ids = df['ch_code'].unique().tolist()
        address_df = address(debtor_ids)
        contact_df = contact(debtor_ids)
        st.write(contact_df)

        st.success("Data fetched successfully!")
        
        # Update contact_dict processing
        contact_dict = (
            contact_df.groupby("ch_code")["number"]
            .apply(lambda x: prioritize_phones(x.tolist()))  # Ensure numbers are properly split
            .to_dict()
        )

        # Assign phone numbers correctly
        df["phone1"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[0])
        df["phone2"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[1])
        df["phone3"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[2])
        df["phone4"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[3])
        df["phone5"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[4])

        # Merge addresses
        address_dict = address_df.groupby("ch_code")["address"].apply(list).to_dict()
        df["address1"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[0])
        df["address2"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[1] if len(address_dict.get(x, [])) > 1 else "")
        df["address3"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[2] if len(address_dict.get(x, [])) > 2 else "")
        df["address4"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[3] if len(address_dict.get(x, [])) > 3 else "")
        df["address5"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[4] if len(address_dict.get(x, [])) > 4 else "")

        # Create account information as a dictionary inside a list [{}]
        df["account_information"] = df.apply(lambda row: json.dumps([{
            "TAGGED USER": str(row["collector"]),
            "OB": str(row["outstanding_balance"]),
            "PRINCIPAL": str(row["principal"]),
            "INTEREST": str(row["interest"]),
            "LPA": "",  # Add appropriate value if needed
            "LPD": "",  # Add appropriate value if needed
            "CREDIT LIMIT": str(row["credit_limit"]),
            "CYCLE": str(row["cycle"]),
            "BLOCK CODE": "",
            "PRODUCT TYPE": str(row["product_type"]),
            "EMPLOYER": "",
            "PRIMARY ADDRESS": row["address1"],
            "SECONDARY ADDRESS": row["address2"],
            "TERTIARY ADDRESS": row["address3"],
            "NEW ADDRESS": "",
            "HOME PHONE": row["phone1"],
            "WORK PHONE": row["phone2"],
            "MOBILE PHONE": row["phone3"],
            "NEW CONTACT": "",
            "EMAIL": str(row["email"]),  # Add email if available
            "BIRTHDAY": str(row["birthday"]),  # Add birthday if available
            "NEW OB": "",
            "NEW CUTOFF": ""
        }]), axis=1)

        # Fill missing columns
        extra_columns = ["ptp_amount", "ptp_date_start", "ptp_date_end", "or_number", "new_contact", 
                         "new_email_address", "source_type", "agent", "new_address", "notes", 
                        "additional_information", "field_result_information", "history_information"]
        for col in extra_columns:
            df[col] = ""

        # Filter columns for export
        columns = ["ch_code", "name", "ch_name", "account_number", "outstanding_balance", "principal", 
                   "endorsement_date", "cutoff_date", "phone1", "phone2", "phone3", "phone4", "phone5", 
                   "address1", "address2", "address3", "address4", "address5", "account_information"] + extra_columns
        df_filtered = df[columns]

        st.write("Filtered Data:")
        st.write(df_filtered)