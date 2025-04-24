
import psycopg2
from psycopg2 import Error
import pandas as pd
from dotenv import load_dotenv
import os
from utils.db import *
from utils.function import *
from datetime import datetime
import zipfile

class ExtractAmeyo:
    def __init__(self):
        # Get the project root directory (BCP2)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        # Path to .env file in the config folder
        env_path = os.path.join(project_root, 'config', '.env')

        # Load environment variables from .env file
        if not os.path.exists(env_path):
            raise FileNotFoundError(f".env file not found at {env_path}")
        load_dotenv(env_path)
        
        # Database configuration from environment variables
        self.db_config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME')
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
            # Explicitly pass connection parameters to avoid defaults
            connection = psycopg2.connect(
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database']
            )
            print(f"Connected to database: {self.db_config['database']} at {self.db_config['host']}:{self.db_config['port']}")
            return connection
        except (Exception, Error) as error:
            print(f"Error connecting to PostgreSQL: {error}")
            raise

    def execute_query(self, cursor):
        """Execute the query and return results."""
        try:
            cursor.execute(self.query)
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            return rows, colnames
        except (Exception, Error) as error:
            print(f"Error executing query: {error}")
            raise

    def init_ftp(self, df_filtered, selected_client, chunk_size):
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
                    print(f"FTP credentials missing for server {server['hostname'] or 'unknown'}")
                    continue  # Skip this server if credentials are incomplete

                print(f"Connecting to server: {server['hostname']}")
                # Establish FTP connection
                ftp = connect_to_ftp(server["hostname"], server["port"], server["username"], server["password"])
                if ftp is None:
                    print(f"Failed to connect to FTP server {server['hostname']}")
                    continue

                try:
                    # Ensure ftp_base_remote_path exists
                    print(f"Ensuring base path exists: {ftp_base_remote_path}")
                    current_path = "/"
                    path_components = [p for p in ftp_base_remote_path.split("/") if p]
                    for component in path_components:
                        current_path = os.path.join(current_path, component).replace("\\", "/")
                        try:
                            ftp.cwd(current_path)  # Try to navigate to the directory
                        except:
                            try:
                                print(f"Creating directory: {current_path}")
                                ftp.mkd(current_path)
                                ftp.cwd(current_path)
                            except Exception as e:
                                if "550" in str(e):
                                    print(f"Permission denied creating {current_path} on {server['hostname']}. Check FTP user permissions.")
                                else:
                                    print(f"Failed to create {current_path} on {server['hostname']}: {e}")
                                raise Exception(f"Unable to ensure base path {ftp_base_remote_path} on {server['hostname']}") from e

                    print(f"Base path {ftp_base_remote_path} is ready")

                    # Proceed with upload
                    print(f"Uploading to server: {server['hostname']}")
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
                    print(f"Error processing FTP server {server['hostname']}: {e}")
                    continue  # Continue with the next server
                finally:
                    try:
                        ftp.quit()
                    except:
                        pass

            print(f"📤 FTP upload completed for {selected_client}")

        except Exception as e:
            print(f"Error in init_ftp: {e}")
            raise

    def upload_to_ftp(self, df, hostname, port, username, password, base_remote_path, filename_base, selected_client, chunk_size):
        """Upload all DataFrame chunks as CSV and XLSX files in a single ZIP to an FTP server."""
        try:
            print("🔍 Checking directory structure...")
            current_date = datetime.now()
            year = current_date.strftime("%Y")
            month = current_date.strftime("%b")
            client_folder = selected_client.lower()
            remote_path = os.path.join(base_remote_path, year, month, client_folder, "AMEYO").replace("\\", "/")

            print("🔌 Connecting to FTP server...")
            ftp = connect_to_ftp(hostname, port, username, password)
            if ftp is None:
                raise Exception("FTP connection failed")

            current_path = base_remote_path
            for folder in [year, month, client_folder, "AMEYO"]:
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
        """Fetch and display query results."""
        connection = None
        cursor = None
        try:
            # Establish connection
            connection = self.get_connection()
            cursor = connection.cursor()

            # Execute query and fetch results
            rows, colnames = self.execute_query(cursor)

            # Create and display DataFrame
            df = pd.DataFrame(rows, columns=colnames)
            print("\nQuery Results:")
            print(df.to_string(index=False))
            chunk_size = 5000
            self.init_ftp(df, "Sample", chunk_size)
  

        except (Exception, Error) as error:
            print(f"Error in display: {error}")
            raise
        
        finally:
            # Close database connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                print("\nPostgreSQL connection closed.")
