import streamlit as st
import psycopg2
from psycopg2 import Error
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
import zipfile
import io
from utils.db import connect_to_ftp  # Assuming this utility exists
import uuid

# Streamlit page configuration
# st.set_page_config(page_title="Ameyo Data Extractor")

class ExtractAmeyo:
    def __init__(self, db_name):
        # Path to .env file
        env_path = "/home/ubuntu/bcp/config/.env"

        # Load environment variables
        if not os.path.exists(env_path):
            st.error(f".env file not found at {env_path}")
            raise FileNotFoundError(f".env file not found at {env_path}")
        load_dotenv(env_path)

        # Database configuration
        self.db_config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': db_name
        }

        # SQL query
        self.query = """
        SELECT  
            ch_code AS "DEBTOR ID",
            account_number AS "ACCOUNT NUMBER",
            ch_name AS "NAME",
            CONCAT(disposition_status, ' - ', (regexp_split_to_array(disposition, ' - '))[1]) AS "STATUS CODE",
            notes AS "REMARKS",
            agent AS "REMARKS BY",
            TO_CHAR(call_date::timestamp, 'YYYY-MM-DD HH24:MI:SS') AS "REMARKS DATE",
            phoneoriginal AS "PHONE NO",
            TO_CHAR(NULLIF(ptp_date_start, '')::date, 'YYYY-MM-DD') AS "PTP DATE",
            ptp_amount AS "PTP AMOUNT",
            TO_CHAR(NULLIF(ptp_date_start, '')::date, 'YYYY-MM-DD') AS "CLAIM PAID DATE",
            ptp_amount AS "CLAIM PAID AMOUNT"
        FROM customer_history
        WHERE disposition_status IS NOT NULL;
        """

    def get_connection(self):
        """Establish and return a database connection."""
        try:
            connection = psycopg2.connect(
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database']
            )
            st.success(f"Connected to database: {self.db_config['database']}")
            return connection
        except (Exception, Error) as error:
            st.error(f"Error connecting to PostgreSQL: {error}")
            raise

    def execute_query(self, cursor):
        """Execute the query and return results."""
        try:
            cursor.execute(self.query)
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            return rows, colnames
        except (Exception, Error) as error:
            st.error(f"Error executing query: {error}")
            raise

    def init_ftp(self, df_filtered, selected_client, chunk_size):
        """Initialize FTP upload process."""
        try:
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
            ftp_base_remote_path = "/admin/ACTIVE/backup/LEADS"
            filename_base = f"{selected_client}-{pd.Timestamp.now().strftime('%Y-%m-%d')}"

            for server in ftp_servers:
                if not all([server["hostname"], server["username"], server["password"]]):
                    st.warning(f"FTP credentials missing for server {server['hostname'] or 'unknown'}")
                    continue

                st.info(f"Connecting to FTP server: {server['hostname']}")
                ftp = connect_to_ftp(server["hostname"], server["port"], server["username"], server["password"])
                if ftp is None:
                    st.error(f"Failed to connect to FTP server {server['hostname']}")
                    continue

                try:
                    current_path = "/"
                    path_components = [p for p in ftp_base_remote_path.split("/") if p]
                    for component in path_components:
                        current_path = os.path.join(current_path, component).replace("\\", "/")
                        try:
                            ftp.cwd(current_path)
                        except:
                            try:
                                ftp.mkd(current_path)
                                ftp.cwd(current_path)
                            except Exception as e:
                                st.error(f"Failed to create {current_path} on {server['hostname']}: {e}")
                                raise

                    self.upload_to_ftp(
                        df_filtered,
                        server["hostname"],
                        server["port"],
                        server["username"],
                        server["password"],
                        ftp_base_remote_path,
                        filename_base,
                        selected_client,
                        chunk_size
                    )
                except Exception as e:
                    st.error(f"Error processing FTP server {server['hostname']}: {e}")
                    continue
                finally:
                    try:
                        ftp.quit()
                    except:
                        pass

            st.success(f"FTP upload completed for {selected_client}")

        except Exception as e:
            st.error(f"Error in init_ftp: {e}")
            raise

    def upload_to_ftp(self, df, hostname, port, username, password, base_remote_path, filename_base, selected_client, chunk_size):
        """Upload DataFrame chunks as CSV and XLSX files in a ZIP to FTP."""
        try:
            current_date = datetime.now()
            year = current_date.strftime("%Y")
            month = current_date.strftime("%b")
            client_folder = selected_client.lower()
            remote_path = os.path.join(base_remote_path, year,"AMEYO", month, client_folder).replace("\\", "/")

            ftp = connect_to_ftp(hostname, port, username, password)
            if ftp is None:
                raise Exception("FTP connection failed")

            current_path = base_remote_path
            for folder in [year, "AMEYO", month, client_folder]:
                current_path = os.path.join(current_path, folder).replace("\\", "/")
                try:
                    ftp.cwd(current_path)
                except:
                    ftp.mkd(current_path)

            ftp.cwd(remote_path)

            total_rows = len(df)
            num_chunks = (total_rows + chunk_size - 1) // chunk_size
            st.info(f"Splitting data into {num_chunks} chunk(s)...")

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for i in range(num_chunks):
                    start_idx = i * chunk_size
                    end_idx = min((i + 1) * chunk_size, total_rows)
                    chunk = df.iloc[start_idx:end_idx]

                    part_suffix = f"_part{i+1}" if num_chunks > 1 else ""
                    csv_filename = f"{filename_base}{part_suffix}.csv"
                    xlsx_filename = f"{filename_base}{part_suffix}.xlsx"

                    csv_buffer = io.StringIO()
                    chunk.to_csv(csv_buffer, index=False)
                    zipf.writestr(csv_filename, csv_buffer.getvalue())

                    xlsx_buffer = io.BytesIO()
                    chunk.to_excel(xlsx_buffer, index=False, engine='openpyxl')
                    zipf.writestr(xlsx_filename, xlsx_buffer.getvalue())

            zip_base_name = f"{filename_base}.zip"
            existing_files = ftp.nlst()
            zip_filename = zip_base_name
            counter = 1
            while zip_filename in existing_files:
                zip_filename = f"{filename_base}({counter}).zip"
                counter += 1

            zip_buffer.seek(0)
            ftp.storbinary(f"STOR {zip_filename}", zip_buffer)
            st.success(f"Uploaded `{zip_filename}` to `{remote_path}`")
            ftp.quit()

        except Exception as e:
            st.error(f"Failed to upload files to FTP: {str(e)}")

    def display(self, selected_client, chunk_size=5000):
        """Fetch and display query results."""
        connection = None
        cursor = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            rows, colnames = self.execute_query(cursor)
            df = pd.DataFrame(rows, columns=colnames)
            
            # Check if DataFrame is empty
            if df.empty:
                st.warning("No data returned from the query. FTP upload will not proceed.")
                return None
            
            st.write("### Query Results")
            st.dataframe(df, use_container_width=True)
            self.init_ftp(df, selected_client, chunk_size)
            return df
        except (Exception, Error) as error:
            st.error(f"Error in display: {error}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                st.info("PostgreSQL connection closed.")

def get_cms_databases():
    """Fetch list of databases with 'cms_' prefix."""
    try:
        temp_config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': 'postgres'  # Connectmei to default database to list others
        }
        connection = psycopg2.connect(**temp_config)
        cursor = connection.cursor()
        cursor.execute("SELECT datname FROM pg_database WHERE datname LIKE 'cms_%';")
        databases = [row[0] for row in cursor.fetchall()]
        cursor.close()
        connection.close()
        return sorted(databases)
    except Exception as e:
        st.error(f"Error fetching databases: {e}")
        return []

def ameyo_main():
    st.title("Ameyo Data Extractor")
    databases = get_cms_databases()
    
    if not databases:
        st.error("No databases with prefix 'cms_' found.")
        return

    selected_db = st.selectbox("Select Database", databases)
    chunk_size = st.number_input("Chunk Size", min_value=1000, value=5000, step=1000)

    if st.button("Extract and Upload"):
        with st.spinner("Processing..."):
            extractor = ExtractAmeyo(selected_db)
            df = extractor.display(selected_db, chunk_size)
            if df is not None:
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv_buffer.getvalue(),
                    file_name=f"{selected_db}-{pd.Timestamp.now().strftime('%Y-%m-%d')}.csv",
                    mime="text/csv"
                )

# if __name__ == "__main__":
#     main()