import os
import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError
from utils.db import db_engine
from utils.function import *
import json
import io
from datetime import datetime

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

        # st.write(select_clause)
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
    
# def dar(debtor_ids):
#     """Fetch current month's data from the database."""
#     try:
#         id_list = ', '.join(map(str, debtor_ids))
#         volare = db_engine('volare')
        # sql_query = f"""
            # SELECT
            #    debtor.id AS 'ch_code',
            #    followup.datetime AS "RESULT DATE",
            #    debtor.collector_user_name AS "AGENT",
            #    followup.status_code AS "STATUS CODE",
            #    SUBSTRING_INDEX(followup.status_code, ' - ', 1) AS 'DISPOSITION',
            #    SUBSTRING_INDEX(followup.status_code, ' - ', -1) AS 'SUB DISPOSITION',
            #    debtor.balance AS "AMOUNT",
            #    debtor_followup.ptp_amount AS 'PTP AMOUNT',
            #    DATE_FORMAT(debtor_followup.ptp_date, '%d/%m/%Y') AS 'PTP DATE',
            #    debtor_followup.claim_paid_amount AS 'CLAIM PAID AMOUNT',
            #    DATE_FORMAT(debtor_followup.claim_paid_date, '%d/%m/%Y') AS 'CLAIM PAID DATE',
            #    followup.remark AS "NOTES",
            #    followup.contact_number AS "NUMBER CONTACTED",
            #    followup.remark_by AS "BARCODED BY"
            # FROM debtor
            #     LEFT JOIN debtor_followup ON debtor_followup.debtor_id = debtor.id
            #     LEFT JOIN followup ON followup.id = debtor_followup.followup_id
            #     LEFT JOIN `user` ON `user`.id = followup.remark_by_id
            #     LEFT JOIN `client` ON client.id = debtor.client_id
            #     LEFT JOIN debtor_address ON debtor_address.debtor_id = debtor.id
            #     LEFT JOIN `address` ON address.id = debtor_address.address_id
            # WHERE client.id IN ('75')
            #     AND debtor.is_aborted <> 1
            #     AND debtor.is_locked <> 1
            #     AND followup.datetime >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
            #     AND YEAR(followup.datetime) = YEAR(CURRENT_DATE)
            #     AND MONTH(followup.datetime) = MONTH(CURRENT_DATE)
            #     AND debtor.id IN ({id_list})
#         """
#         st.code = (sql_query)
#         df = fetch_data(sql_query, volare)
       
#         return df
#     except SQLAlchemyError as e:
#         st.error(f"Error fetching data: {e}")
#         return None
    
def dar(debtor_ids):
    """Fetch the 10 latest dispositions per debtor account from the database."""
    try:
        id_list = ', '.join(map(str, debtor_ids))
        volare = db_engine('volare')
        sql_query = f"""
            WITH RankedDispositions AS (
                SELECT
                    debtor.id AS 'ch_code',
                    followup.datetime AS "RESULT DATE",
                    debtor.collector_user_name AS "AGENT",
                    followup.status_code AS "STATUS CODE",
                    SUBSTRING_INDEX(followup.status_code, ' - ', 1) AS 'DISPOSITION',
                    SUBSTRING_INDEX(followup.status_code, ' - ', -1) AS 'SUB DISPOSITION',
                    debtor.balance AS "AMOUNT",
                    debtor_followup.ptp_amount AS 'PTP AMOUNT',
                    DATE_FORMAT(debtor_followup.ptp_date, '%d/%m/%Y') AS 'PTP DATE',
                    debtor_followup.claim_paid_amount AS 'CLAIM PAID AMOUNT',
                    DATE_FORMAT(debtor_followup.claim_paid_date, '%d/%m/%Y') AS 'CLAIM PAID DATE',
                    followup.remark AS "NOTES",
                    followup.contact_number AS "NUMBER CONTACTED",
                    followup.remark_by AS "BARCODED BY",
                    ROW_NUMBER() OVER (PARTITION BY debtor.id ORDER BY followup.datetime DESC) AS rn
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
                    AND debtor.id IN ({id_list})
                    AND followup.datetime IS NOT NULL  -- Ensure RESULT DATE is not NULL
            )
            SELECT
                ch_code,
                `RESULT DATE`,  -- Use backticks for column names with spaces
                AGENT,
                `STATUS CODE`,
                DISPOSITION,
                `SUB DISPOSITION`,
                AMOUNT,
                `PTP AMOUNT`,
                `PTP DATE`,
                `CLAIM PAID AMOUNT`,
                `CLAIM PAID DATE`,
                NOTES,
                `NUMBER CONTACTED`,
                `BARCODED BY`
            FROM RankedDispositions
            WHERE rn <= 10;
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

@st.cache_data
def process_data(selected_client, config_path):
    df = info(selected_client,config_path)
    
    if df is not None:
        debtor_ids = df['ch_code'].unique().tolist()
        address_df = address(debtor_ids)
        contact_df = contact(debtor_ids)
        dar_raw = dar(debtor_ids)

        dar_df = remove_data(dar_raw, status_code_col='STATUS CODE', remark_col='NOTES')

        st.success("Data fetched successfully!")

        date_columns = ["birthday", "endorsement_date", "cutoff_date"]
        df[date_columns] = df[date_columns].apply(lambda x: pd.to_datetime(x, errors='coerce')).apply(lambda x: x.dt.strftime('%m/%d/%Y'))
        
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

        # Load mappings from config
        mappings = load_mappings(selected_client, config_path)
        mapped_columns = [mapped_col for _, mapped_col in mappings]  # Get only the Mapped Column names

        # Define fixed fields for account_information
        fixed_account_fields = {
            "TAGGED USER": "collector",
            "OB": "outstanding_balance",
            "PRINCIPAL": "principal",
            "CARD_NO": "card_no",
            "PLACEMENT": "placement",
            "CYCLE": "cycle",
            "PRODUCT TYPE": "product_type",
            "PRIMARY ADDRESS": "address1",
            "SECONDARY ADDRESS": "address2",
            "TERTIARY ADDRESS": "address3",
            "PHONE1": "phone1",
            "PHONE2": "phone2",
            "PHONE3": "phone3",
            "PHONE4": "phone4",
            "PHONE5": "phone5"
        }

        # Columns to exclude from additional_information
        account_cols = list(fixed_account_fields.values())
        additional_exclusions = ["ch_code", "name", "ch_name", "account_number", "outstanding_balance", 
                                "principal", "endorsement_date", "cutoff_date"]
        excluded_cols = account_cols + additional_exclusions

        # Dynamic account_information with fixed fields, replacing NaN with ""
        def create_account_info(row):
            account_dict = {}
            for key, col in fixed_account_fields.items():
                if col in row.index:
                    value = row[col]
                    account_dict[key] = "" if pd.isna(value) else str(value)
            return json.dumps([account_dict])

        df["account_information"] = df.apply(create_account_info, axis=1)

        # Dynamic additional_information, replacing NaN with ""
        def create_additional_info(row):
            additional_dict = {}
            for col in mapped_columns:
                if col not in excluded_cols and col in row.index:
                    value = row[col]
                    additional_dict[col.upper()] = "" if pd.isna(value) else str(value)
            return json.dumps([additional_dict])

        df["additional_information"] = df.apply(create_additional_info, axis=1)

        # Process dar_df for history_information (latest 10 dispositions)
        dar_columns = {
            "RESULT DATE": "RESULT DATE",
            "AGENT": "AGENT",
            "DISPOSITION": "DISPOSITION",
            "SUB DISPOSITION": "SUB DISPOSITION",
            "AMOUNT": "AMOUNT",
            "PTP AMOUNT": "PTP AMOUNT",
            "PTP DATE": "PTP DATE",
            "CLAIM PAID AMOUNT": "CLAIM PAID AMOUNT",
            "CLAIM PAID DATE": "CLAIM PAID DATE",
            "NOTES": "NOTES",
            "NUMBER CONTACTED": "NUMBER CONTACTED",
            "BARCODED BY": "BARCODED BY"
        }

        # Convert dates in dar_df to strings
        dar_df["RESULT DATE"] = pd.to_datetime(dar_df["RESULT DATE"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        dar_df["PTP DATE"] = pd.to_datetime(dar_df["PTP DATE"], format='%d/%m/%Y', errors='coerce').dt.strftime('%m/%d/%y')
        dar_df["CLAIM PAID DATE"] = pd.to_datetime(dar_df["CLAIM PAID DATE"], format='%d/%m/%Y', errors='coerce').dt.strftime('%m/%d/%y')

        # Sort dar_df by RESULT DATE descending and take top 10 per ch_code, replacing NaN with ""
        dar_df_sorted = dar_df.sort_values("RESULT DATE", ascending=False)
        dar_grouped = dar_df_sorted.groupby("ch_code").apply(
            lambda x: [
                {key: "" if pd.isna(row[i]) else str(row[i]) for i, key in enumerate(dar_columns.keys())}
                for row in x[list(dar_columns.values())].values
            ][:10]
        ).to_dict()

        # Add history_information to df
        df["history_information"] = df["ch_code"].map(lambda x: json.dumps(dar_grouped.get(x, [])))

        # Fill missing columns
        extra_columns = ["ptp_amount", "ptp_date_start", "ptp_date_end", "or_number", "new_contact", 
                         "new_email_address", "source_type", "agent", "new_address", "notes", 
                         ]
        for col in extra_columns:
            df[col] = ""
        df["field_result_information"] = ""
        # Filter columns for export and replace NaN with ""
        columns = ["ch_code", "name", "ch_name", "account_number", "outstanding_balance", "principal", 
                   "endorsement_date", "cutoff_date", "phone1", "phone2", "phone3", "phone4", "phone5", 
                   "address1", "address2", "address3", "address4", "address5"] + extra_columns + ["account_information", "additional_information","field_result_information","history_information"]
        df_filtered = df[columns].fillna("")
        return df_filtered
    return None

# Streamlit UI
st.title("📊 Database Query & Export Tool")
config_path="/home/spm/Documents/BCP2/config/config.xlsx"
xls = pd.ExcelFile(config_path)
available_clients = xls.sheet_names 

selected_client = st.selectbox("Select a Client", available_clients)

if selected_client:
    create_btn = st.button("Fetch Data")
else:
    st.warning("No clients available in the config file.")

# Initialize session state to track if data has been fetched
if 'data_fetched' not in st.session_state:
    st.session_state['data_fetched'] = False
if 'df_filtered' not in st.session_state:
    st.session_state['df_filtered'] = None

if create_btn:
    st.session_state['data_fetched'] = True
    st.session_state['df_filtered'] = process_data(selected_client, config_path)
    if st.session_state['df_filtered'] is not None:
        st.success("Data fetched successfully!")
    else:
        st.error("Failed to fetch data.")

# Display data and download buttons if data has been fetched
if st.session_state['data_fetched'] and st.session_state['df_filtered'] is not None:
    df_filtered = st.session_state['df_filtered']
    
    st.write("Filtered Data:")
    st.write(df_filtered)

    # Generate file name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = f"{selected_client}_data_{timestamp}"

    # Prepare CSV file
    csv_buffer = io.StringIO()
    df_filtered.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Prepare XLSX file
    xlsx_buffer = io.BytesIO()
    df_filtered.to_excel(xlsx_buffer, index=False, engine='openpyxl')
    xlsx_data = xlsx_buffer.getvalue()

    # Download buttons
    st.download_button(
        label="Download as CSV",
        data=csv_data,
        file_name=f"{base_filename}.csv",
        mime="text/csv",
        key="csv_download"
    )

    st.download_button(
        label="Download as Excel",
        data=xlsx_data,
        file_name=f"{base_filename}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="xlsx_download"
    )

    # Optional: Auto-trigger download (comment out if not desired)
    st.markdown(f"""
        <script>
        document.getElementById('csv_download').click();
        document.getElementById('xlsx_download').click();
        </script>
    """, unsafe_allow_html=True)