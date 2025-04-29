import os
import sqlite3
import pytz  # Import pytz for time zone handling
from datetime import datetime, timedelta

MAX_USERS = 10
DB_PATH = 'active_users.db'

def create_table_if_not_exists():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS active_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_token TEXT NOT NULL,
                email TEXT NOT NULL,
                login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        print("Table created or already exists.")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")
    finally:
        conn.close()


# Ensure table creation happens when the script runs
if not os.path.exists(DB_PATH):
    print(f"Database file {DB_PATH} does not exist. Creating the table.")
    create_table_if_not_exists()
else:
    print(f"Database file {DB_PATH} exists. Checking table creation.")
    create_table_if_not_exists()

# Function to get the count of active users
def get_active_user_count():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM active_users")
    count = cursor.fetchone()[0]
    conn.close()
    return count

# Function to add a new active user to the database
def add_active_user(user_token, email):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    manila_tz = pytz.timezone('Asia/Manila')
    login_time = datetime.now(manila_tz)
    login_time_str = login_time.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        cursor.execute(
            "INSERT INTO active_users (user_token, email, login_time) VALUES (?, ?, ?)",
            (user_token, email, login_time_str)
        )
        conn.commit()
        print(f"User with email {email} added.")
    except sqlite3.Error as e:
        print(f"Error adding user: {e}")
    finally:
        conn.close()


def is_email_active(email):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM active_users WHERE email = ?", (email,))
        count = cursor.fetchone()[0]
        return count > 0
    except sqlite3.Error as e:
        print(f"Error checking user email: {e}")
        return False
    finally:
        conn.close()

# Function to remove an active user from the database
def remove_active_user(user_token):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM active_users WHERE user_token = ?", (user_token,))
        conn.commit()  # Ensure changes are saved
        print(f"User with token {user_token} removed.")
    except sqlite3.Error as e:
        print(f"Error removing user: {e}")
    finally:
        conn.close()

# Function to clean up users who have been inactive for more than 30 minutes
def cleanup_inactive_users():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get the current time in Manila timezone (PHT)
    manila_tz = pytz.timezone('Asia/Manila')
    thirty_minutes_ago = datetime.now(manila_tz) - timedelta(minutes=30)
    
    # Delete users whose login time is more than 30 minutes ago
    try:
        cursor.execute("DELETE FROM active_users WHERE login_time < ?", (thirty_minutes_ago,))
        conn.commit()
        print("Inactive users cleaned up.")
    except sqlite3.Error as e:
        print(f"Error cleaning up inactive users: {e}")
    finally:
        conn.close()

# Ensure the user limit is not exceeded
def check_user_limit():
    active_users_count = get_active_user_count()
    if active_users_count >= MAX_USERS:
        return False
    return True
