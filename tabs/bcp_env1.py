# bcp_automation.py
import os
import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError
import json
from datetime import datetime
import zipfile
from time import time
from utils.db import *
from utils.function import *

class BCPAutomation:
    def __init__(self):
        self.config_path = "/home/spm/Documents/BCP2/config/config.xlsx"

    @st.cache_data(ttl=600)
    def client_id(_self, selected_env, selected_port):
        """Fetch current month's contact data from the database in chunks."""
        try:
            volare = db_engine('volare', selected_port)
            start_time = time()
            sql_query = read_sql_file("query/fetch_clients.sql")
            df = fetch_data(sql_query, volare)

            total_time = time() - start_time
            print(f"All CLIENTS have been fetched for {selected_env} ✅ Total time: {total_time:.2f} seconds.")

            if df is not None and not df.empty:
                return df
            else:
                print("No clients found.")
                return None

        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return None
    
    def active(self, selected_client_id, selected_port):
        try:
            volare = db_engine('volare', selected_port)
            start_time = time()
            sql_template = read_sql_file("query/fetch_active.sql")
            sql_query = sql_template.format(selected_client_id=selected_client_id)
            df = fetch_data(sql_query, volare)

            total_time = time() - start_time
            print(f"All Active accounts have been fetched  ✅ Total time: {total_time:.2f} seconds.")

            if df is not None and not df.empty:
                return df
            else:
                print("No clients found.")
                return None

        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return None


    def info(self, ids, selected_client, selected_client_id, selected_port):
        """Fetch data for the selected client using dynamic column mappings, handling large queries in chunks."""
        # debtor_ids = ids['Acct_Num'].dropna().unique().tolist()
        debtor_ids = ids
        
        if not debtor_ids:
            print("No valid debtor IDs found.")
            return None

        try:
            volare = db_engine('volare', selected_port)
            # status_text = st.empty()

            mappings = load_mappings("Info", self.config_path)
            if not mappings:
                print(f"No mappings found for {selected_client}.")
                return None

            select_clause = ",\n".join([f"{db_col} AS '{mapped_col}'" for db_col, mapped_col in mappings])
            all_data = []
            total_chunks = (len(debtor_ids) // 10000) + 1
            start_time = time()

            sql_template = read_sql_file("query/fetch_info.sql")

            for idx, chunk in enumerate(chunk_list(debtor_ids, 10000), start=1):
                id_list = ', '.join(f"'{id}'" for id in chunk)
                sql_query = sql_template.format(
                    select_clause=select_clause,
                    selected_client_id=selected_client_id,
                    id_list=id_list
                )

                print(f"Processing chunk {idx}/{total_chunks} ({len(chunk)} records)...")
                df_chunk = fetch_data(sql_query, volare)
                if df_chunk is not None and not df_chunk.empty:
                    all_data.append(df_chunk)

            total_time = time() - start_time
            print(f"Processing INFO completed ✅ Total time: {total_time:.2f} seconds.")

            if all_data:
                final_df = pd.concat(all_data, ignore_index=True)
                return final_df
            else:
                print("No data found for the given debtor IDs.")
                return None

        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return None

    def _fetch_data_in_chunks(self, debtor_ids, selected_client_id, selected_port, 
                            sql_file, chunk_size, process_name):
        """Helper method to fetch data in chunks from database."""
        if not debtor_ids:
            print(f"No debtor IDs provided for {process_name} query. Skipping query.")
            return None
        
        try:
            volare = db_engine('volare', selected_port)
            # status_text = st.empty()
            all_data = []
            total_chunks = (len(debtor_ids) // chunk_size) + 1
            start_time = time()
            sql_template = read_sql_file(sql_file)

            for idx, chunk in enumerate(chunk_list(debtor_ids, chunk_size), start=1):
                id_list = ', '.join(map(str, chunk))
                sql_query = sql_template.format(selected_client_id=selected_client_id, id_list=id_list)
                print(f"Processing chunk {idx}/{total_chunks} ({len(chunk)} records)...")
                df_chunk = fetch_data(sql_query, volare)
                if df_chunk is not None and not df_chunk.empty:
                    all_data.append(df_chunk)

            total_time = time() - start_time
            print(f"Processing {process_name} completed ✅ Total time: {total_time:.2f} seconds.")

            return pd.concat(all_data, ignore_index=True) if all_data else None
            
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return None

    def contact(self, debtor_ids, selected_client_id, selected_port):
        """Fetch current month's contact data."""
        return self._fetch_data_in_chunks(debtor_ids, selected_client_id, selected_port,
                                        "query/fetch_contact.sql", 10000, "Contact")

    def address(self, debtor_ids, selected_client_id, selected_port):
        """Fetch current month's address data."""
        return self._fetch_data_in_chunks(debtor_ids, selected_client_id, selected_port,
                                        "query/fetch_address.sql", 10000, "Address")

    def dar(self, debtor_ids, selected_client_id, selected_port):
        """Fetch the 10 latest dispositions per debtor account."""
        return self._fetch_data_in_chunks(debtor_ids, selected_client_id, selected_port,
                                        "query/fetch_dar.sql", 5000, "DAR")

    def process_data(self, selected_client, selected_client_id, selected_port):
        df_active = self.active(selected_client_id, selected_port)
    
        # Check if df_active is None and return None if true
        if df_active is None:
            print(f"No active data found for client ID {selected_client_id}. Returning None.")
            return None
            
        ids = df_active['Acct_Num'].dropna().unique().tolist()
        print(f"Debtor IDs: {ids.__len__()}")
        print(df_active)
        df = self.info(ids, selected_client, selected_client_id, selected_port)
        try:
            if not df.empty:
                debtor_ids = df['ch_code'].dropna().unique().tolist()
                address_df = self.address(debtor_ids, selected_client_id, selected_port)
                contact_df = self.contact(debtor_ids, selected_client_id, selected_port)
                dar_raw = self.dar(debtor_ids, selected_client_id, selected_port)

                # status_text = st.empty()
                print("Processing Templated Data...")
                start_time = time()

                dar_df = dar_raw.copy()
                dar_df = remove_data(dar_raw, status_code_col='STATUS CODE', remark_col='NOTES')
                dar_df.loc[:, 'NOTES'] = dar_df['NOTES'].str.replace('\n', ' ', regex=False)
               
                date_columns = ["birthday", "endorsement_date", "cutoff_date"]
                df[date_columns] = df[date_columns].apply(lambda x: pd.to_datetime(x, errors='coerce')).apply(lambda x: x.dt.strftime('%m/%d/%Y'))

                df["phone1"] = ''
                df["phone2"] = ''
                df["phone3"] = ''
                df["phone4"] = ''
                df["phone5"] = ''

                if contact_df is not None:
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

                df["address1"] = ''
                df["address2"] = ''
                df["address3"] = ''
                df["address4"] = ''
                df["address5"] = ''

                if address_df is not None:
                    address_dict = address_df.groupby("ch_code")["address"].apply(list).to_dict()
                    df["address1"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[0])
                    df["address2"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[1] if len(address_dict.get(x, [])) > 1 else "")
                    df["address3"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[2] if len(address_dict.get(x, [])) > 2 else "")
                    df["address4"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[3] if len(address_dict.get(x, [])) > 3 else "")
                    df["address5"] = df["ch_code"].map(lambda x: address_dict.get(x, [""])[4] if len(address_dict.get(x, [])) > 4 else "")
                
                mappings = load_mappings(selected_client, self.config_path)
                mapped_columns = [mapped_col for _, mapped_col in mappings]
               
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
                print( "21",df.columns)
                columns = ["ch_code", "name", "ch_name", "account_number", "outstanding_balance", "principal", 
                         "endorsement_date", "cutoff_date", "phone1", "phone2", "phone3", "phone4", "phone5", 
                         "address1", "address2", "address3", "address4", "address5"] + extra_columns + ["account_information", "additional_information", "field_result_information", "history_information"]
                df_filtered = df[columns].fillna("")
              
                total_time = time() - start_time
                print(f"Processing Templated Data Completed ✅ Total time: {total_time:.2f} seconds.")
                return df_filtered
        
        except Exception as e:
            print(f"Error fetching data")
            return None

    def init_ftp(self, df_filtered, selected_client, chunk_size):
        try:
            ftp_hostname = os.getenv("FTP_HOSTNAME")
            ftp_port = int(os.getenv("FTP_PORT", 21))
            ftp_username = os.getenv("FTP_USERNAME")
            ftp_password = os.getenv("FTP_PASSWORD")
            # ftp_base_remote_path = "/admin/ACTIVE/backup/LEADS"
            ftp_base_remote_path = "/admins/RPA OUTPUT/GENERAL/BCP LEADS"
            filename_base = f"{selected_client}-{pd.Timestamp.now().strftime('%Y-%m-%d')}"

            if not all([ftp_hostname, ftp_username, ftp_password]):
                print("FTP credentials (hostname, username, or password) are missing from the .env file.")
            else:
                self.upload_to_ftp(df_filtered, ftp_hostname, ftp_port, ftp_username, ftp_password, ftp_base_remote_path, filename_base, selected_client, chunk_size)
            # status.update(label="Report creation completed!", state="complete")

        except Exception as e:
            print(f"Error in remove_data: {e}")
            raise

    def upload_to_ftp(self, df, hostname, port, username, password, base_remote_path, filename_base, selected_client, chunk_size):
        """Upload all DataFrame chunks as CSV and XLSX files in a single ZIP to an FTP server."""
        try:
            print("🔍 Checking directory structure...")
            current_date = datetime.now()
            year = current_date.strftime("%Y")
            month = current_date.strftime("%b")
            client_folder = selected_client.lower()
            remote_path = os.path.join(base_remote_path, year, month, client_folder).replace("\\", "/")

            print("🔌 Connecting to FTP server...")
            ftp = connect_to_ftp(hostname, port, username, password)
            if ftp is None:
                raise Exception("FTP connection failed")

            current_path = base_remote_path
            for folder in [year, month, client_folder]:
                current_path = os.path.join(current_path, folder).replace("\\", "/")
                try:
                    ftp.cwd(current_path)
                except:
                    ftp.mkd(current_path)
                    print(f"📁 Created directory: {current_path}")

            ftp.cwd(remote_path)

            total_rows = len(df)
            num_chunks = (total_rows + chunk_size - 1) // chunk_size

            print(f"📊 Splitting data into {num_chunks} chunk(s)...")
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

            zip_base_name = f"{filename_base}.zip"
            zip_temp_file = f"/tmp/{zip_base_name}"
            print("📦 Compressing files into ZIP...")
            with zipfile.ZipFile(zip_temp_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for temp_file, arcname in temp_files:
                    zipf.write(temp_file, arcname)

            existing_files = ftp.nlst()
            zip_filename = zip_base_name
            counter = 1
            while zip_filename in existing_files:
                zip_filename = f"{filename_base}({counter}).zip"
                counter += 1

            print(f"🚀 Uploading `{zip_filename}`...")
            with open(zip_temp_file, 'rb') as f:
                ftp.storbinary(f"STOR {zip_filename}", f)

            print(f"✅ Uploaded `{zip_filename}` to:")
            print(f"`{remote_path}`")

            for temp_file, _ in temp_files:
                os.remove(temp_file)
            os.remove(zip_temp_file)
            ftp.quit()

        except Exception as e:
            print(f"❌ Failed to upload files to FTP: {str(e)}")

    def display(self):
        print("📤 CMS - AMEYO")

        # Static chunk size and environment
        chunk_size = 5000
        selected_env = "ENV1"
        env_port_mapping = {"ENV1": 3306, "ENV2": 3307, "ENV3": 3308}
        selected_port = env_port_mapping[selected_env]

        print(f"Selected environment: {selected_env}")
        print(f"Using port: {selected_port}")
        print(f"Chunk size: {chunk_size}\n")

        # Fetch client list
        # client_df = self.client_id(selected_env, selected_port)
        # client_df = pd.DataFrame({
        #     'name': [
        #         'PRELEGAL BDO CARDS', 'LEGAL BNB MSME', 'LEGAL BNB SL', 'LEGAL BPI BSL', 'LEGAL RCBC',
        #         'LEGAL SBC', 'PRELEGAL BNB MSME', 'PRELEGAL BNB SL', 'PRELEGAL BPI BSL', 'PRELEGAL BPI CARDS',
        #         'PRELEGAL HSBC', 'PRELEGAL RCBC', 'PRELEGAL SBC', 'LEGAL CSB MCL', 'PRELEGAL CSB MCL'
        #     ],
        #     'id': [
        #         100, 103, 104, 106, 109, 110, 113, 114, 115, 116, 118, 119, 120, 126, 127
        #     ]
        # })

        client_df = pd.DataFrame({
            'name': [
                'RCBC'
                
            ],
            'id': [
                 84
            ]
        })


        if client_df is not None and not client_df.empty:
            client_dict = dict(zip(client_df['name'], client_df['id']))
            print(f"📋 Found {len(client_dict)} clients. Starting data processing...\n")

            for client_name, client_id in client_dict.items():
                print(f"🔄 Processing client: {client_name} (ID: {client_id})")

                # Always load mappings from the 'Info' sheet
                mappings = load_mappings("Info", self.config_path)
                if not mappings:
                    print("⚠️ No mappings found in the 'Info' sheet. Skipping.\n")
                    continue

                df_filtered = self.process_data(client_name, client_id, selected_port)
                print(f"📊 Data processed for: {df_filtered}")

                if df_filtered is None:
                    print(f"⚠️ No active data found for {client_name}. Skipping to next client.\n")
                    continue

                if not df_filtered.empty:
                    print(f"✅ Data fetched for {client_name}. Sending to FTP...")
                    self.init_ftp(df_filtered, client_name, chunk_size)
                    print(f"📤 FTP upload completed for {client_name}\n")
                else:
                    print(f"⚠️ No data returned for {client_name}. Skipping.\n")
        else:
            print("❌ No clients available for this environment.")

