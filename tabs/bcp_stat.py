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
        self.config_path = "/home/ubuntu/bcp/config/config.xlsx"

    def client_id(_self, selected_env, selected_port):
        """Fetch current month's contact data from the database in chunks."""
        try:
            volare = db_engine('volare', selected_port)
            start_time = time()
            sql_query = read_sql_file("/home/ubuntu/bcp/query/fetch_clients.sql")
            df = fetch_data(sql_query, volare)

            total_time = time() - start_time


            if df is not None and not df.empty:
                # st.write(df)
                st.success(f"All CLIENTS have been fetched for {selected_env} ✅ Total time: {total_time:.2f} seconds.")
                return df
            else:
                st.write("No clients found.")
                return None

        except SQLAlchemyError as e:
            st.write(f"Database error: {e}")
            return None
    
    def active(self, selected_client_id, selected_port):
        try:
            volare = db_engine('volare', selected_port)
            start_time = time()
            sql_template = read_sql_file("/home/ubuntu/bcp/query/fetch_active.sql")
            sql_query = sql_template.format(selected_client_id=selected_client_id)
            df = fetch_data(sql_query, volare)

            total_time = time() - start_time
            st.write(f"All Active accounts have been fetched  ✅ Total time: {total_time:.2f} seconds.")

            if df is not None and not df.empty:
                return df
            else:
                st.write("No active accounts found.")
                return None

        except SQLAlchemyError as e:
            st.write(f"Database error: {e}")
            return None

    def _fetch_data_in_chunks(self, debtor_ids, selected_client_id, selected_port, 
                            sql_file, chunk_size, process_name):
        """Helper method to fetch data in chunks from database."""
        if not debtor_ids:
            st.write(f"No debtor IDs provided for {process_name} query. Skipping query.")
            return None
        
        try:
            volare = db_engine('volare', selected_port)
            status_text = st.empty()
            all_data = []
            total_chunks = (len(debtor_ids) // chunk_size) + 1
            start_time = time()
            sql_template = read_sql_file(sql_file)

            for idx, chunk in enumerate(chunk_list(debtor_ids, chunk_size), start=1):
                id_list = ', '.join(map(str, chunk))
                sql_query = sql_template.format(selected_client_id=selected_client_id, id_list=id_list)
                status_text.text(f"Processing chunk {idx}/{total_chunks} ({len(chunk)} records)...")
                df_chunk = fetch_data(sql_query, volare)
                if df_chunk is not None and not df_chunk.empty:
                    all_data.append(df_chunk)

            total_time = time() - start_time
            status_text.text(f"Processing {process_name} completed ✅ Total time: {total_time:.2f} seconds.")

            return pd.concat(all_data, ignore_index=True) if all_data else None
            
        except SQLAlchemyError as e:
            st.warning(f"Database error: {e}")
            return None

    def dar(self, debtor_ids, selected_client_id, selected_port):
        """Fetch the 10 latest dispositions per debtor account."""
        return self._fetch_data_in_chunks(debtor_ids, selected_client_id, selected_port,
                                        "/home/ubuntu/bcp/query/fetch_stat.sql", 5000, "DAR")

    def process_data(self, selected_client, selected_client_id, selected_port):
        df_active = self.active(selected_client_id, selected_port)
    
        # Check if df_active is None and return None if true
        if df_active is None:
            st.write(f"No active data found for client ID {selected_client_id}. Returning None.")
            return None
            
        ids = df_active['id'].dropna().unique().tolist()
        st.code(f"Active Accounts: {ids.__len__()}")
        try:
            dar_raw = self.dar(ids, selected_client_id, selected_port)
            
            status_text = st.empty()
            status_text.text("Processing Templated Data...")
            start_time = time()
            
            # dar_df = dar_raw.copy()
            dar_raw = remove_data(dar_raw, status_code_col='STATUS CODE', remark_col='REMARKS')
            st.code(f"Total Cleaned Efforts: {dar_raw.__len__()}")    
            dar_raw['PTP AMOUNT'] = pd.to_numeric(dar_raw['PTP AMOUNT'], errors='coerce').fillna(0).astype(int)
            dar_raw['CLAIM PAID AMOUNT'] = pd.to_numeric(dar_raw['CLAIM PAID AMOUNT'], errors='coerce').fillna(0).astype(int)

            dar_raw.loc[:, 'REMARKS'] = dar_raw['REMARKS'].str.replace('\n', ' ', regex=False)
            dar_raw.loc[:, "BARCODE DATE"] = pd.to_datetime(dar_raw["BARCODE DATE"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            dar_raw.loc[:, "PTP DATE"] = pd.to_datetime(dar_raw["PTP DATE"], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
            dar_raw.loc[:, "CLAIM PAID DATE"] = pd.to_datetime(dar_raw["CLAIM PAID DATE"], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
            df_filtered = dar_raw.replace([float("inf"), float("-inf"), pd.NA, "NA", None], "").fillna("")
            # df_filtered = dar_df.fillna("")
            
            total_time = time() - start_time
            status_text.text(f"Processing Templated Data Completed ✅ Total time: {total_time:.2f} seconds.")
            return df_filtered
        
        except Exception as e:
            st.warning(f"Error fetching data")
            return None


    def init_ftp(self, df_filtered, selected_client, chunk_size,status):
        try:
            # Define the FTP server configurations
            ftp_servers = [
                {
                    "hostname": os.getenv("NMKT_FTP_HOSTNAME"),
                    "port": int(os.getenv("NMKT_FTP_PORT", 21)),
                    "username": os.getenv("NMKT_FTP_USERNAME"),
                    "password": os.getenv("NMKT_FTP_PASSWORD")
                },
                {
                    "hostname": os.getenv("PITX_FTP_HOSTNAME"),
                    "port": int(os.getenv("PITX_FTP_PORT", 21)),
                    "username": os.getenv("PITX_FTP_USERNAME"),
                    "password": os.getenv("PITX_FTP_PASSWORD")
                },
                {
                    "hostname": os.getenv("PAN_FTP_HOSTNAME"),
                    "port": int(os.getenv("PAN_FTP_PORT", 21)),
                    "username": os.getenv("PAN_FTP_USERNAME"),
                    "password": os.getenv("PAN_FTP_PASSWORD")
                }
            ]
            # Common FTP settings
            ftp_base_remote_path = "/admin/ACTIVE/backup/LEADS"
            filename_base = f"{selected_client}-{pd.Timestamp.now().strftime('%Y-%m-%d')}"

            # Iterate through each FTP server
            for server in ftp_servers:
                if not all([server["hostname"], server["username"], server["password"]]):
                    st.write(f"FTP credentials missing for server {server['hostname'] or 'unknown'}")
                    continue  # Skip this server if credentials are incomplete

                # st.write(f"Connecting to server: {server['hostname']}")
                # Establish FTP connection
                ftp = connect_to_ftp(server["hostname"], server["port"], server["username"], server["password"])
                if ftp is None:
                    st.write(f"Failed to connect to FTP server {server['hostname']}")
                    continue
                st.write(f"✅ Successfully connected to FTP server at {server["hostname"]}:{server["port"]}")
                try:
                    # Ensure ftp_base_remote_path exists
                    # st.write(f"Ensuring base path exists: {ftp_base_remote_path}")
                    current_path = "/"
                    path_components = [p for p in ftp_base_remote_path.split("/") if p]
                    for component in path_components:
                        current_path = os.path.join(current_path, component).replace("\\", "/")
                        try:
                            ftp.cwd(current_path)  # Try to navigate to the directory
                        except:
                            try:
                                # st.write(f"Creating directory: {current_path}")
                                ftp.mkd(current_path)
                                ftp.cwd(current_path)
                            except Exception as e:
                                if "550" in str(e):
                                    st.write(f"Permission denied creating {current_path} on {server['hostname']}. Check FTP user permissions.")
                                else:
                                    st.write(f"Failed to create {current_path} on {server['hostname']}: {e}")
                                raise Exception(f"Unable to ensure base path {ftp_base_remote_path} on {server['hostname']}") from e

                    # st.write(f"Base path {ftp_base_remote_path} is ready")

                    # Proceed with upload
                    # st.write(f"Uploading to server: {server['hostname']}")
                    self.upload_to_ftp(
                        df_filtered,
                        server["hostname"],
                        server["port"],
                        server["username"],
                        server["password"],
                        ftp_base_remote_path,
                        filename_base,
                        selected_client,
                        chunk_size,
                        status
                    )
                except Exception as e:
                    st.write(f"Error processing FTP server {server['hostname']}: {e}")
                    continue  # Continue with the next server
                finally:
                    try:
                        ftp.quit()
                    except:
                        pass

        except Exception as e:
            st.write(f"Error in init_ftp: {e}")
            raise

    def upload_to_ftp(self, df, hostname, port, username, password, base_remote_path, filename_base, selected_client, chunk_size, status):
        """Upload all DataFrame chunks as CSV and XLSX files in a single ZIP to an FTP server."""
        try:
            st.write("🔍 Checking directory structure...")
            current_date = datetime.now()
            year = current_date.strftime("%Y")
            month = current_date.strftime("%b")
            client_folder = selected_client.lower()
            remote_path = os.path.join(base_remote_path, year, "CMS - AUTOSTAT",  month, client_folder).replace("\\", "/")

            st.write(f"🔌 Directory `{remote_path}` is ready")
            ftp = connect_to_ftp(hostname, port, username, password)
            if ftp is None:
                raise Exception("FTP connection failed")

            current_path = base_remote_path
            for folder in [year, "CMS - AUTOSTAT", month, client_folder]:
                current_path = os.path.join(current_path, folder).replace("\\", "/")
                try:
                    ftp.cwd(current_path)
                except:
                    ftp.mkd(current_path)
                    st.write(f"📁 Created directory: {current_path}")

            ftp.cwd(remote_path)

            total_rows = len(df)
            num_chunks = (total_rows + chunk_size - 1) // chunk_size

            st.write(f"📊 Splitting data into {num_chunks} chunk(s)...")
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
            st.write("📦 Compressing files into ZIP...")
            with zipfile.ZipFile(zip_temp_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for temp_file, arcname in temp_files:
                    zipf.write(temp_file, arcname)

            existing_files = ftp.nlst()
            zip_filename = zip_base_name
            counter = 1
            while zip_filename in existing_files:
                zip_filename = f"{filename_base}({counter}).zip"
                counter += 1
     
            st.write(f"🚀 Uploading `{zip_filename}`...")
            with open(zip_temp_file, 'rb') as f:
                ftp.storbinary(f"STOR {zip_filename}", f)

            st.write(f"✅ Uploaded `{zip_filename}` to: `{remote_path}`")
            st.write(f"====================================================================================")
            status.update(label="Report creation completed!", state="complete")
            for temp_file, _ in temp_files:
                os.remove(temp_file)
            os.remove(zip_temp_file)
            ftp.quit()

        except Exception as e:
            st.write(f"❌ Failed to upload files to FTP: {str(e)}")

    def display(self):
        st.header("📤 CMS LATEST STATUS")
        
        chunk_size = st.number_input("Enter Chunk Size:", min_value=1, value=5000, step=100)

        env_options = ["ENV1", "ENV2", "ENV3"]
        env_port_mapping = {"ENV1": 3306, "ENV2": 3307, "ENV3": 3308}

        selected_env = st.selectbox("Select Environment", env_options)
        selected_port = env_port_mapping[selected_env]

        if selected_env:
            client_df = self.client_id(selected_env, selected_port)
        else:
            st.warning("No clients available.")

        if client_df is not None and not client_df.empty:
            client_dict = dict(zip(client_df['name'], client_df['id']))
            selected_client = st.selectbox("Select Client", options=client_dict.keys())
            selected_client_id = client_dict[selected_client]
        else:
            st.warning("No clients available.")

        # st.info("Note: Excel file should have column('accountid') with a value of debtor account number.")
        # file = st.file_uploader("Upload File", type='xlsx')

        # if selected_client and selected_client_id and file and selected_env:
        if selected_client and selected_client_id  and selected_env:
            create_btn = st.button("Get Data")
            
            if create_btn:
                with st.status("Creating report...", expanded=True) as status:
                    # debtor_id = get_raw_file(file)
                    # df_filtered = self.process_data(debtor_id, selected_client, selected_client_id, selected_port)
                    df_filtered = self.process_data(selected_client, selected_client_id, selected_port)
                    if df_filtered is not None:
                        st.success("Data processed successfully! Ready to upload.")
                        st.write("Final Data:")
                        st.write(df_filtered.head())
                        self.init_ftp(df_filtered, selected_client, chunk_size,status)
                    else:
                        st.error("No data fetched.")
                        status.update(label="Report creation failed!", state="error")
        else:
            st.warning("Please select a client and upload the needed file.")