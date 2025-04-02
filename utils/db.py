import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import paramiko
import streamlit as st
from ftplib import FTP
def db_engine(credential_type, port=None):
    try:
     
        # Get the absolute path of the current file's directory
        current_working_dir = os.getcwd()
       
        env_path = os.path.join(current_working_dir, 'config', '.env')
        load_dotenv(dotenv_path=env_path)
      
        # Convert credential_type to lowercase for consistency
        credential_type = credential_type.lower()

        # Fields required from the environment variables
        credentials_fields = ['host', 'user', 'pass', 'db']
        
        # Retrieve credentials from environment variables
        creds = {}
        for field in credentials_fields:
            env_var = f"{credential_type}_{field.lower()}"
            value = os.getenv(env_var)
            if value is None:
                raise ValueError(f"Missing environment variable: {env_var}")
            creds[field] = quote_plus(value)
        
        # Set default port if not provided
        if port is None:
            port = os.getenv(f"{credential_type}_PORT", 3306)
        creds['port'] = port
        
        # Construct the engine URL with the port
        engine_url = 'mysql+pymysql://{user}:{pass}@{host}:{port}/{db}'
        
        engine_credentials = engine_url.format(**creds)
        # Create and return the SQLAlchemy engine
        return create_engine(engine_credentials)
    
    except Exception as e:
        error_message = f"An error occurred while creating the database engine: {str(e)}"
        print(error_message)  # For logging
        return None

def connect_to_ftp(hostname, port, username, password):
    """Establish a connection to the FTP server and return the FTP client."""
    try:
        # Initialize FTP client
        ftp = FTP()
        
        # Debugging: Log connection attempt
        # st.write(f"Attempting to connect to {hostname}:{port} as {username}...")
        
        # Connect and login
        ftp.connect(host=hostname, port=port, timeout=10)
        ftp.login(user=username, passwd=password)
        
        st.write(f"✅ Successfully connected to FTP server at {hostname}:{port}")
        return ftp
        
    except Exception as e:
        st.error(f"Failed to connect to FTP: {str(e)}")
        return None