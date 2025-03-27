import os
import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError
from utils.db import *
from utils.function import *
import json
from datetime import datetime
import zipfile


def load_mappings(client_name, config_path):
    """Load column mappings from the sheet corresponding to the selected client."""
    try:
        df_client_mappings = pd.read_excel(config_path, sheet_name=client_name)

        # Convert to list of tuples to keep duplicates
        mappings = list(zip(df_client_mappings["Database Column"], df_client_mappings["Mapped Column"]))

        return mappings
    except ValueError:
        st.error(f"No sheet found for {client_name} in {config_path}")
        return []
    
def info(selected_client, selected_client_id, selected_port, config_path):
    """Fetch data for the selected client using dynamic column mappings."""
    try:
        volare = db_engine('volare', selected_port)
        st.text(f"Fetching data for active accounts")

        # Load dynamic mappings from the selected client's sheet
        mappings = load_mappings(selected_client, config_path)

        if not mappings:
            st.warning(f"No mappings found for {selected_client}.")
            return None

        # Generate the SELECT clause dynamically
        select_clause = ",\n".join([f"{db_col} AS '{mapped_col}'" for db_col, mapped_col in mappings])

        # st.write(select_clause)  # Corrected: Use st.write as a function
        sql_query = f"""
            SELECT DISTINCT
                {select_clause}
            FROM debtor
                LEFT JOIN `client` ON client.id = debtor.client_id
            WHERE client.id IN ({selected_client_id})
                AND debtor.is_aborted <> 1
                AND debtor.is_locked <> 1
             -- AND debtor.account = '0006081000158864'
        """
        # st.code(sql_query)  # Corrected: Use st.write as a function
        df = fetch_data(sql_query, volare)
        return df
    except SQLAlchemyError as e:
        return None
    
def contact(debtor_ids, selected_client_id, selected_port):
    """Fetch current month's data from the database."""
    if not debtor_ids:
        st.warning("No debtor IDs provided for address query. Skipping query.")
        return None
    try:
        st.text(f"Fetching data for contact numbers")
        id_list = ', '.join(map(str, debtor_ids))
        volare = db_engine('volare', selected_port)
        sql_query = f"""
            SELECT DISTINCT
                debtor.id AS 'ch_code',
                contact_number.contact_number AS 'number'
            FROM debtor
                LEFT JOIN debtor_followup ON debtor_followup.debtor_id = debtor.id
                LEFT JOIN followup ON followup.id = debtor_followup.followup_id
                LEFT JOIN `client` ON client.id = debtor.client_id
                LEFT JOIN contact_number ON contact_number.id = followup.contact_number_id
            WHERE client.id IN ({selected_client_id})
                AND debtor.is_aborted <> 1
                AND debtor.is_locked <> 1
                AND debtor.id IN ({id_list})
                AND contact_number.contact_number <> 'NA'
                AND contact_number.contact_number IS NOT NULL
        """
        # st.code(sql_query)  # Corrected: Use st.code as a function
        df = fetch_data(sql_query, volare)
       
        return df
    except SQLAlchemyError as e:
        st.error(f"Error fetching data: {e}")
        return None

def address(debtor_ids, selected_client_id, selected_port):
    """Fetch current month's data from the database."""
    if not debtor_ids:
        st.warning("No debtor IDs provided for address query. Skipping query.")
        return None
    try:
        st.text(f"Fetching data for list of addresses")
        id_list = ', '.join(map(str, debtor_ids))
        volare = db_engine('volare', selected_port)
        sql_query = f"""
            SELECT DISTINCT
                debtor.id AS 'ch_code',
                address.address AS 'address'
            FROM debtor
                LEFT JOIN `client` ON client.id = debtor.client_id
                LEFT JOIN debtor_address ON debtor_address.debtor_id = debtor.id
                LEFT JOIN `address` ON address.id = debtor_address.address_id
            WHERE client.id IN ({selected_client_id})
                AND debtor.is_aborted <> 1
                AND debtor.is_locked <> 1
                AND debtor.id IN ({id_list})  -- Filter by debtor IDs
                AND address.address <> 'NA'
                AND address.address IS NOT NULL
        """
        # st.code(sql_query)  # Corrected: Use st.code as a function
        df = fetch_data(sql_query, volare)
       
        return df
    except SQLAlchemyError as e:
        st.error(f"Error fetching data: {e}")
        return None
    
def dar(debtor_ids, selected_client_id, selected_port):
    """Fetch the 10 latest dispositions per debtor account from the database."""
    if not debtor_ids:
        st.warning("No debtor IDs provided for address query. Skipping query.")
        return None
    try:
        st.text(f"Fetching data for recent dispositions")
        id_list = ', '.join(map(str, debtor_ids))
        volare = db_engine('volare', selected_port)
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
                    CASE  
                        WHEN followup.remark_type_id = 1 THEN 'Follow Up' 
                        WHEN followup.remark_type_id = 2 THEN 'Internal Remark'  
                        WHEN followup.remark_type_id = 3 THEN 'Payment'
                        WHEN followup.remark_type_id = 4 THEN 'SMS'
                        WHEN followup.remark_type_id = 5 THEN 'Field Visit'
                        WHEN followup.remark_type_id = 6 THEN 'Legal'
                        WHEN followup.remark_type_id = 7 THEN 'Letter Attachment & Email'
                        WHEN followup.remark_type_id = 9 THEN 'Permanent Message'
                    END AS 'CONTACT SOURCE',
                    ROW_NUMBER() OVER (PARTITION BY debtor.id ORDER BY followup.datetime DESC) AS rn
                FROM debtor
                    LEFT JOIN debtor_followup ON debtor_followup.debtor_id = debtor.id
                    LEFT JOIN followup ON followup.id = debtor_followup.followup_id
                    LEFT JOIN `client` ON client.id = debtor.client_id
                WHERE client.id IN ({selected_client_id})
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
                `BARCODED BY`,
                `CONTACT SOURCE`
            FROM RankedDispositions
            WHERE rn <= 10;
        """
        # st.code(sql_query)  # Corrected: Use st.code as a function
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

def process_data(selected_client, selected_client_id, selected_port, config_path):
    df = info(selected_client, selected_client_id, selected_port, config_path)
    try:
        if not df.empty:
            debtor_ids = df['ch_code'].unique().tolist()
            address_df = address(debtor_ids, selected_client_id, selected_port)
            contact_df = contact(debtor_ids, selected_client_id, selected_port)
            dar_raw = dar(debtor_ids, selected_client_id, selected_port)

            # Assuming remove_data is defined elsewhere in utils.function
            dar_df = dar_raw.copy()
            dar_df = remove_data(dar_raw, status_code_col='STATUS CODE', remark_col='NOTES')
            dar_df.loc[dar_df["DISPOSITION"] == "BULK SMS SENT", "NOTES"] = ""
            st.write(dar_df)
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
                "BARCODED BY": "BARCODED BY",
                "CONTACT SOURCE": "CONTACT SOURCE"
            }

            # Convert dates in dar_df to strings
            dar_df["RESULT DATE"] = pd.to_datetime(dar_df["RESULT DATE"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            dar_df["PTP DATE"] = pd.to_datetime(dar_df["PTP DATE"], format='%d/%m/%Y', errors='coerce').dt.strftime('%m/%d/%y')
            dar_df["CLAIM PAID DATE"] = pd.to_datetime(dar_df["CLAIM PAID DATE"], format='%d/%m/%Y', errors='coerce').dt.strftime('%m/%d/%y')

            # Sort dar_df by RESULT DATE descending and take top 10 per ch_code, replacing NaN with ""
            dar_df_sorted = dar_df.sort_values("RESULT DATE", ascending=False).copy()

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
                            "new_email_address", "source_type", "agent", "new_address", "notes"]
            for col in extra_columns:
                df[col] = ""
            df["field_result_information"] = ""
            # Filter columns for export and replace NaN with ""
            columns = ["ch_code", "name", "ch_name", "account_number", "outstanding_balance", "principal", 
                    "endorsement_date", "cutoff_date", "phone1", "phone2", "phone3", "phone4", "phone5", 
                    "address1", "address2", "address3", "address4", "address5"] + extra_columns + ["account_information", "additional_information", "field_result_information", "history_information"]
            df_filtered = df[columns].fillna("")
            return df_filtered
        
    except Exception as e:
        st.error(f"Error fetching data")

def upload_to_ftp(df, hostname, port, username, password, base_remote_path, filename_base, selected_client):
    """Upload all DataFrame chunks as CSV and XLSX files in a single ZIP to an FTP server."""
    try:
        # Get current year and month
        current_date = datetime.now()
        year = current_date.strftime("%Y")
        month = current_date.strftime("%b")
        client_folder = selected_client.lower()

        # Construct dynamic remote path
        remote_path = os.path.join(base_remote_path, year, month, client_folder).replace("\\", "/")

        # Connect to FTP
        ftp = connect_to_ftp(hostname, port, username, password)
        if ftp is None:
            raise Exception("FTP connection failed")

        # Create directories if they don't exist
        current_path = base_remote_path
        for folder in [year, month, client_folder]:  # Matches your "sample cms" folder
            current_path = os.path.join(current_path, folder).replace("\\", "/")
            try:
                ftp.cwd(current_path)
            except:
                ftp.mkd(current_path)
                st.write(f"Created directory: {current_path}")

        # Change to the target directory
        ftp.cwd(remote_path)

        # Chunk the DataFrame into 5,000-row segments
        chunk_size = 2000
        total_rows = len(df)
        num_chunks = (total_rows + chunk_size - 1) // chunk_size  # Ceiling division

        # List to store temporary file paths
        temp_files = []

        # Generate all CSV and XLSX files
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            chunk = df.iloc[start_idx:end_idx]

            # Generate filenames for CSV and XLSX
            part_suffix = f"_part{i+1}" if num_chunks > 1 else ""
            csv_filename = f"{filename_base}{part_suffix}.csv"
            xlsx_filename = f"{filename_base}{part_suffix}.xlsx"
            csv_temp_file = f"/tmp/{csv_filename}"
            xlsx_temp_file = f"/tmp/{xlsx_filename}"

            # Save chunk as CSV
            chunk.to_csv(csv_temp_file, index=False)
            # Save chunk as XLSX
            chunk.to_excel(xlsx_temp_file, index=False, engine='openpyxl')

            # Add files to the list
            temp_files.append((csv_temp_file, csv_filename))
            temp_files.append((xlsx_temp_file, xlsx_filename))

        zip_base_name = f"{filename_base}.zip"
        zip_temp_file = f"/tmp/{zip_base_name}"

        with zipfile.ZipFile(zip_temp_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for temp_file, arcname in temp_files:
                zipf.write(temp_file, arcname)

        # Check for existing files and generate a unique ZIP filename
        existing_files = ftp.nlst()  # List files in the remote directory
        zip_filename = zip_base_name
        counter = 1
        while zip_filename in existing_files:
            zip_filename = f"{filename_base}({counter}).zip"
            counter += 1

        # Upload the ZIP file with the unique name
        with open(zip_temp_file, 'rb') as f:
            ftp.storbinary(f"STOR {zip_filename}", f)
        st.success(f"Uploaded {zip_filename} to {remote_path}")

        # Clean up temporary files
        for temp_file, _ in temp_files:
            os.remove(temp_file)
        os.remove(zip_temp_file)

        # Clean up FTP connection
        ftp.quit()

    except Exception as e:
        st.error(f"Failed to upload files to FTP: {str(e)}")

# Streamlit UI
st.title("📊 Database Query & Export Tool")
config_path = "/home/spm/Documents/BCP2/config/config.xlsx"
xls = pd.ExcelFile(config_path)
# Load env_client.xlsx and find the client ID
env_client_path = "/home/spm/Documents/BCP2/config/env_client.xlsx"
env_client_xls = pd.ExcelFile(env_client_path)

available_clients = xls.sheet_names 

selected_client = st.selectbox("Select a Client", available_clients)

env_options = ["ENV1", "ENV2", "ENV3"]
env_port_mapping = {
    "ENV1": 3306,
    "ENV2": 3307,
    "ENV3": 3308
}

# Environment selection
if selected_client:
    selected_env = st.selectbox("Select Environment", env_options)
    selected_port = env_port_mapping[selected_env]

    if selected_env in env_client_xls.sheet_names:
        env_df = pd.read_excel(env_client_xls, sheet_name=selected_env)
        client_row = env_df[env_df['name'] == selected_client]
        if not client_row.empty:
            selected_client_id = str(client_row['id'].iloc[0])
            st.write(f"Selected Client ID for {selected_client} in {selected_env}:{selected_port}: {selected_client_id}")
        else:
            st.error(f"Client {selected_client} not found in {selected_env} sheet.")
            selected_client_id = None
    else:
        st.error(f"Environment {selected_env} not found in env_client.xlsx.")
        selected_client_id = None

if selected_client and selected_client_id:
    create_btn = st.button("Fetch Data")
    if create_btn:
        df_filtered = process_data(selected_client, selected_client_id, selected_port, config_path)
        if df_filtered is not None:
            st.success("Data fetched successfully!")
            st.write("Filtered Data:")
            st.write(df_filtered)

            # Load FTP credentials from .env, default to port 21 (FTP default)
            ftp_hostname = os.getenv("FTP_HOSTNAME")
            ftp_port = int(os.getenv("FTP_PORT", 21))  # Default to 21
            ftp_username = os.getenv("FTP_USERNAME")
            ftp_password = os.getenv("FTP_PASSWORD")
            ftp_base_remote_path = "/admins/RPA OUTPUT/GENERAL/BCP LEADS"
            filename_base = f"{selected_client}-{pd.Timestamp.now().strftime('%Y-%m-%d')}"

            st.write(f"FTP Connection Details: {ftp_hostname}:{ftp_port} as {ftp_username}")

            if not all([ftp_hostname, ftp_username, ftp_password]):
                st.error("FTP credentials (hostname, username, or password) are missing from the .env file.")
            else:
                upload_to_ftp(df_filtered, ftp_hostname, ftp_port, ftp_username, ftp_password, ftp_base_remote_path, filename_base,selected_client)
        else:
            st.error("No fetch data.")
else:
    st.warning("Please select a client and ensure the client ID is found in the selected environment.")