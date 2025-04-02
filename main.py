import os
import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError
from utils.db import *
from utils.function import *
import json
from datetime import datetime
import zipfile
import time

@st.cache_data(ttl=600) 
def client_id(selected_env, selected_port):
    """Fetch current month's contact data from the database in chunks."""   
    try:
        volare = db_engine('volare', selected_port)
        start_time = time.time()
        
        sql_query = """
            SELECT DISTINCT 
                client.name, 
                client.id
            FROM `client`
            WHERE client.name NOT LIKE '%TEST%'
                AND client.name NOT LIKE  '%Stampede%'
                AND client.deleted_at IS NULL
        """
        df = fetch_data(sql_query, volare)

        total_time = time.time() - start_time
        st.success(f"All CLIENTS has been fetched for {selected_env} ✅ Total time: {total_time:.2f} seconds.")

        # ✅ Fix: Use `df.empty` to check if it's empty
        if df is not None and not df.empty:
            return df
        else:
            st.warning("No clients found.")
            return None

    except SQLAlchemyError as e:
        st.error(f"Database error: {e}")
        return None

def info(ids, selected_client, selected_client_id, selected_port, config_path):
    """Fetch data for the selected client using dynamic column mappings, handling large queries in chunks."""
    debtor_ids = ids['accountid'].dropna().unique().tolist()
    
    if not debtor_ids:
        st.warning("No valid debtor IDs found.")
        return None

    try:
        volare = db_engine('volare', selected_port)
        status_text = st.empty()

        mappings = load_mappings(selected_client, config_path)
        if not mappings:
            st.warning(f"No mappings found for {selected_client}.")
            return None

        select_clause = ",\n".join([f"{db_col} AS '{mapped_col}'" for db_col, mapped_col in mappings])
        all_data = []
        total_chunks = (len(debtor_ids) // 10000) + 1
        start_time = time.time()

        for idx, chunk in enumerate(chunk_list(debtor_ids, 10000), start=1):
            id_list = ', '.join(f"'{id}'" for id in chunk)
            sql_query = f"""
                SELECT DISTINCT
                    {select_clause}
                FROM debtor
                LEFT JOIN `client` ON client.id = debtor.client_id
                WHERE client.id IN ({selected_client_id})
                    AND debtor.is_aborted <> 1
                    AND debtor.is_locked <> 1
                    AND debtor.account IN ({id_list})
            """
            status_text.text(f"Processing chunk {idx}/{total_chunks} ({len(chunk)} records)...")
            df_chunk = fetch_data(sql_query, volare)
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)

        total_time = time.time() - start_time
        status_text.text(f"Processing INFO completed ✅ Total time: {total_time:.2f} seconds.")

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            return final_df
        else:
            st.warning("No data found for the given debtor IDs.")
            return None

    except SQLAlchemyError as e:
        st.error(f"Database error: {e}")
        return None

def contact(debtor_ids, selected_client_id, selected_port):
    """Fetch current month's contact data from the database in chunks."""
    if not debtor_ids:
        st.warning("No debtor IDs provided for address query. Skipping query.")
        return None
    
    try:
        volare = db_engine('volare', selected_port)
        status_text = st.empty()

        all_data = []
        total_chunks = (len(debtor_ids) // 10000) + 1
        start_time = time.time()

        for idx, chunk in enumerate(chunk_list(debtor_ids, 10000), start=1):
            id_list = ', '.join(map(str, chunk))
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
                    AND debtor.id IN ({id_list})
                    AND contact_number.contact_number <> 'NA'
                    AND contact_number.contact_number IS NOT NULL
                    AND contact_number.deleted_at IS NULL
            """
            status_text.text(f"Processing chunk {idx}/{total_chunks} ({len(chunk)} records)...")
            df_chunk = fetch_data(sql_query, volare)
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)

        total_time = time.time() - start_time
        status_text.text(f"Processing Contact completed ✅ Total time: {total_time:.2f} seconds.")

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            return final_df
        else:
            st.warning("No contact data found for the given debtor IDs.")
            return None

    except SQLAlchemyError as e:
        st.error(f"Database error: {e}")
        return None

def address(debtor_ids, selected_client_id, selected_port):
    """Fetch current month's address data from the database in chunks."""
    if not debtor_ids:
        st.warning("No debtor IDs provided for address query. Skipping query.")
        return None
    
    try:
        volare = db_engine('volare', selected_port)
        status_text = st.empty()

        all_data = []
        total_chunks = (len(debtor_ids) // 10000) + 1
        start_time = time.time()

        for idx, chunk in enumerate(chunk_list(debtor_ids, 10000), start=1):
            id_list = ', '.join(map(str, chunk))
            sql_query = f"""
                SELECT DISTINCT
                    debtor.id AS 'ch_code',
                    address.address AS 'address'
                FROM debtor
                LEFT JOIN `client` ON client.id = debtor.client_id
                LEFT JOIN debtor_address ON debtor_address.debtor_id = debtor.id
                LEFT JOIN `address` ON address.id = debtor_address.address_id
                WHERE client.id IN ({selected_client_id})
                    AND debtor.id IN ({id_list})
                    AND address.address <> 'NA'
                    AND address.address IS NOT NULL
                    AND address.deleted_at IS NULL
            """
            status_text.text(f"Processing chunk {idx}/{total_chunks} ({len(chunk)} records)...")
            df_chunk = fetch_data(sql_query, volare)
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)

        total_time = time.time() - start_time
        status_text.text(f"Processing Address completed ✅ Total time: {total_time:.2f} seconds.")

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            return final_df
        else:
            st.warning("No address data found for the given debtor IDs.")
            return None

    except SQLAlchemyError as e:
        st.error(f"Database error: {e}")
        return None

def dar(debtor_ids, selected_client_id, selected_port):
    """Fetch the 10 latest dispositions per debtor account from the database in chunks."""
    if not debtor_ids:
        st.warning("No debtor IDs provided for disposition query. Skipping query.")
        return None
    
    try:
        volare = db_engine('volare', selected_port)
        status_text = st.empty()

        all_data = []
        total_chunks = (len(debtor_ids) // 5000) + 1
        start_time = time.time()

        for idx, chunk in enumerate(chunk_list(debtor_ids, 5000), start=1):
            id_list = ', '.join(map(str, chunk))
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
                        AND debtor.id IN ({id_list})
                        AND followup.datetime IS NOT NULL
                )
                SELECT
                    ch_code,
                    `RESULT DATE`,  
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
            status_text.text(f"Processing chunk {idx}/{total_chunks} ({len(chunk)} records)...")
            df_chunk = fetch_data(sql_query, volare)
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)

        total_time = time.time() - start_time
        status_text.text(f"Processing DAR completed ✅ Total time: {total_time:.2f} seconds.")

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            return final_df
        else:
            st.warning("No disposition data found for the given debtor IDs.")
            return None

    except SQLAlchemyError as e:
        st.error(f"Database error: {e}")
        return None

def process_data(ids, selected_client, selected_client_id, selected_port, config_path):
    df = info(ids, selected_client, selected_client_id, selected_port, config_path)
    try:
        if not df.empty:
            debtor_ids = df['ch_code'].dropna().unique().tolist()
            address_df = address(debtor_ids, selected_client_id, selected_port)
            contact_df = contact(debtor_ids, selected_client_id, selected_port)
            dar_raw = dar(debtor_ids, selected_client_id, selected_port)

            status_text = st.empty()
            status_text.text("Processing Templated Data...")
            start_time = time.time()

            dar_df = dar_raw.copy()
            dar_df = remove_data(dar_raw, status_code_col='STATUS CODE', remark_col='NOTES')
            dar_df.loc[:, 'NOTES'] = dar_df['NOTES'].str.replace('\n', ' ', regex=False)

            date_columns = ["birthday", "endorsement_date", "cutoff_date"]
            df[date_columns] = df[date_columns].apply(lambda x: pd.to_datetime(x, errors='coerce')).apply(lambda x: x.dt.strftime('%m/%d/%Y'))

            contact_dict = (
                contact_df.groupby("ch_code")["number"]
                .apply(lambda x: prioritize_phones(x.tolist()))
                .to_dict()
            )

            df["phone1"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[0])
            df["phone2"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[1])
            df["phone3"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[2])
            df["phone4"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[3])
            df["phone5"] = df["ch_code"].map(lambda x: contact_dict.get(x, ["", "", "", "", ""])[4])

            df = df.apply(update_phone1, axis=1)
            df = df.apply(fix_phone1, axis=1)
            df = df.apply(format_phone_numbers, axis=1)


            address_dict = address_df.groupby("ch_code")["address"].apply(list).to_dict()
            df["address1"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[0])
            df["address2"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[1] if len(address_dict.get(x, [])) > 1 else "")
            df["address3"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[2] if len(address_dict.get(x, [])) > 2 else "")
            df["address4"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[3] if len(address_dict.get(x, [])) > 3 else "")
            df["address5"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[4] if len(address_dict.get(x, [])) > 4 else "")

            mappings = load_mappings(selected_client, config_path)
            mapped_columns = [mapped_col for _, mapped_col in mappings]  # Get only the Mapped Column names

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

            account_cols = list(fixed_account_fields.values())
            additional_exclusions = ["ch_code", "name", "ch_name", "account_number", "outstanding_balance", 
                                    "principal", "endorsement_date", "cutoff_date"]
            excluded_cols = account_cols + additional_exclusions

            def create_account_info(row):
                account_dict = {}
                for key, col in fixed_account_fields.items():
                    if col in row.index:
                        value = row[col]
                        account_dict[key] = "" if pd.isna(value) else str(value)
                return json.dumps([account_dict])

            df["account_information"] = df.apply(create_account_info, axis=1)

            def create_additional_info(row):
                additional_dict = {}
                for col in mapped_columns:
                    if col not in excluded_cols and col in row.index:
                        value = row[col]
                        additional_dict[col.upper()] = "" if pd.isna(value) else str(value)
                return json.dumps([additional_dict])

            df["additional_information"] = df.apply(create_additional_info, axis=1)

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

            dar_df.loc[:, "RESULT DATE"] = pd.to_datetime(dar_df["RESULT DATE"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            dar_df.loc[:, "PTP DATE"] = pd.to_datetime(dar_df["PTP DATE"], format='%d/%m/%Y', errors='coerce').dt.strftime('%m/%d/%y')
            dar_df.loc[:, "CLAIM PAID DATE"] = pd.to_datetime(dar_df["CLAIM PAID DATE"], format='%d/%m/%Y', errors='coerce').dt.strftime('%m/%d/%y')

            dar_df_sorted = dar_df.sort_values("RESULT DATE", ascending=False).copy()
            dar_grouped = dar_df_sorted.groupby("ch_code").apply(
                lambda x: [
                    {key: "" if pd.isna(row[i]) else str(row[i]) for i, key in enumerate(dar_columns.keys())}
                    for row in x[list(dar_columns.values())].values
                ][:10]
            ).to_dict()


            df["history_information"] = df["ch_code"].map(lambda x: json.dumps(dar_grouped.get(x, [])))

            extra_columns = ["ptp_amount", "ptp_date_start", "ptp_date_end", "or_number", "new_contact", 
                            "new_email_address", "source_type", "agent", "new_address", "notes"]
            for col in extra_columns:
                df[col] = ""
            df["field_result_information"] = ""

            columns = ["ch_code", "name", "ch_name", "account_number", "outstanding_balance", "principal", 
                    "endorsement_date", "cutoff_date", "phone1", "phone2", "phone3", "phone4", "phone5", 
                    "address1", "address2", "address3", "address4", "address5"] + extra_columns + ["account_information", "additional_information", "field_result_information", "history_information"]
            df_filtered = df[columns].fillna("")
 
            total_time = time.time() - start_time
            status_text.text(f"Processing Templated Data Completed ✅ Total time: {total_time:.2f} seconds.")
            return df_filtered
        
    except Exception as e:
        st.error(f"Error fetching data")

def upload_to_ftp(df, hostname, port, username, password, base_remote_path, filename_base, selected_client,chunk_size):
    """Upload all DataFrame chunks as CSV and XLSX files in a single ZIP to an FTP server."""
    try:
        st.text("🔍 Checking directory structure...")
        current_date = datetime.now()
        year = current_date.strftime("%Y")
        month = current_date.strftime("%b")
        client_folder = selected_client.lower()
        remote_path = os.path.join(base_remote_path, year, month, client_folder).replace("\\", "/")

        # Connect to FTP
        st.text("🔌 Connecting to FTP server...")
        ftp = connect_to_ftp(hostname, port, username, password)
        if ftp is None:
            raise Exception("FTP connection failed")

        # Ensure the remote directory exists
        current_path = base_remote_path
        for folder in [year, month, client_folder]:
            current_path = os.path.join(current_path, folder).replace("\\", "/")
            try:
                ftp.cwd(current_path)
            except:
                ftp.mkd(current_path)
                st.text(f"📁 Created directory: {current_path}")

        ftp.cwd(remote_path)

        # Split DataFrame into chunks
        # chunk_size = 2000
        total_rows = len(df)
        num_chunks = (total_rows + chunk_size - 1) // chunk_size

        st.text(f"📊 Splitting data into {num_chunks} chunk(s)...")
        temp_files = []
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            chunk = df.iloc[start_idx:end_idx]

            part_suffix = f"_part{i+1}" if num_chunks > 1 else ""
            csv_filename = f"{filename_base}{part_suffix}.csv"
            xlsx_filename = f"{filename_base}{part_suffix}.xlsx"
            csv_temp_file = f"/tmp/{csv_filename}"
            xlsx_temp_file = f"/tmp/{xlsx_filename}"

            chunk.to_csv(csv_temp_file, index=False)
            chunk.to_excel(xlsx_temp_file, index=False, engine='openpyxl')

            temp_files.append((csv_temp_file, csv_filename))
            temp_files.append((xlsx_temp_file, xlsx_filename))

        # Create ZIP file
        zip_base_name = f"{filename_base}.zip"
        zip_temp_file = f"/tmp/{zip_base_name}"
        st.text("📦 Compressing files into ZIP...")
        with zipfile.ZipFile(zip_temp_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for temp_file, arcname in temp_files:
                zipf.write(temp_file, arcname)

        # Check if the file already exists and rename if necessary
        existing_files = ftp.nlst()
        zip_filename = zip_base_name
        counter = 1
        while zip_filename in existing_files:
            zip_filename = f"{filename_base}({counter}).zip"
            counter += 1

        # Upload ZIP to FTP
        st.text(f"🚀 Uploading `{zip_filename}` to `{remote_path}`...")
        with open(zip_temp_file, 'rb') as f:
            ftp.storbinary(f"STOR {zip_filename}", f)

        st.success(f"✅ Uploaded `{zip_filename}` to:")
        st.code(f"{remote_path}")

        # Cleanup temporary files
        for temp_file, _ in temp_files:
            os.remove(temp_file)
        os.remove(zip_temp_file)
        ftp.quit()

    except Exception as e:
        st.error(f"❌ Failed to upload files to FTP: {str(e)}")

def init_ftp(df_filtered,selected_client,chunk_size):
    try:
        ftp_hostname = os.getenv("FTP_HOSTNAME")
        ftp_port = int(os.getenv("FTP_PORT", 21))
        ftp_username = os.getenv("FTP_USERNAME")
        ftp_password = os.getenv("FTP_PASSWORD")
        ftp_base_remote_path = "/admins/RPA OUTPUT/GENERAL/BCP LEADS"
        filename_base = f"{selected_client}-{pd.Timestamp.now().strftime('%Y-%m-%d')}"

        if not all([ftp_hostname, ftp_username, ftp_password]):
            st.error("FTP credentials (hostname, username, or password) are missing from the .env file.")
        else:
            upload_to_ftp(df_filtered, ftp_hostname, ftp_port, ftp_username, ftp_password, ftp_base_remote_path, filename_base, selected_client,chunk_size)
        status.update(label="Report creation completed!", state="complete")

    except Exception as e:
        st.error(f"Error in remove_data: {e}")
        raise

# Streamlit UI
config_path = "/home/spm/Documents/BCP2/config/config.xlsx"

# Create Tabs
tab1, tab2 = st.tabs(["BCP Automation Tool", "Extract to Ameyo"])

with tab1:
    st.header("📤 CMS - AMEYO")
    
    chunk_size = st.number_input("Enter Chunk Size:", min_value=1, value=5000, step=100)

    env_options = ["ENV1", "ENV2", "ENV3"]
    env_port_mapping = {"ENV1": 3306, "ENV2": 3307, "ENV3": 3308}

    selected_env = st.selectbox("Select Environment", env_options)
    selected_port = env_port_mapping[selected_env]

    if selected_env:
        client_df = client_id(selected_env, selected_port)  # Removed extra selected_env argument
    else:
        st.warning("No clients available.")

    if client_df is not None and not client_df.empty:
        client_dict = dict(zip(client_df['name'], client_df['id']))
        selected_client = st.selectbox("Select Client", options=client_dict.keys())
        selected_client_id = client_dict[selected_client]
    else:
        st.warning("No clients available.")

    st.info("Note: Excel file should have column('accountid') with a value of debtor account number.")
    file = st.file_uploader("Upload File", type='xlsx')

    if selected_client and selected_client_id and file and selected_env:
        create_btn = st.button("Get Data")
        
        if create_btn:
            with st.status("Creating report...", expanded=True) as status:
                debtor_id = get_raw_file(file)
                df_filtered = process_data(debtor_id, selected_client, selected_client_id, selected_port, config_path)

                if df_filtered is not None:
                    st.success("Data fetched successfully!")
                    st.write("Final Data:")
                    st.write(df_filtered)
                    init_ftp(df_filtered, selected_client, chunk_size)
                else:
                    st.error("No data fetched.")
                    status.update(label="Report creation failed!", state="error")
    else:
        st.warning("Please select a client and upload the needed file.")

with tab2:
    st.header("📤 AMEYO - CMS")
    selected_client = st.selectbox("Select Client", options=["Client A", "Client B", "Client C"])
    if selected_client:
        create_btn = st.button("Fetch Data")
        st.success(f"Selected Client: {selected_client}")
        if create_btn:
            with st.status("Creating report...", expanded=True) as status:
                st.success(f"Data fetched successfully for {selected_client}!") 
                status.update(label="Report creation completed!", state="complete")
                
    else:   
        st.warning("Please select a client.")