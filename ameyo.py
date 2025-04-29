import streamlit as st
import json
import re
from streamlit_cookies_controller import CookieController
from time import sleep
import hashlib
from tabs.ameyo_sub import main
from utils.session import *
from utils.lark import *

# Run cleanup every time app runs
cleanup_inactive_users()

auth_url = f'https://open.larksuite.com/open-apis/authen/v1/authorize?app_id={APP_ID}&redirect_uri={REDIRECT_URI}'
mode = 'production'

# st.set_page_config(page_title="BANK REPORT", page_icon='🏛', layout="centered", initial_sidebar_state="auto")

# Custom styling
st.markdown("""
    <style>
        .css-1v3fvcr {
            background-color: #262730;
        }
        .casual-button {
            display: inline-flex;
            align-items: center;
            padding: 12px 20px;
            font-size: 18px;
            text-align: center;
            cursor: pointer;
            background-color: #0D92F4;
            color: #000000;
            border: none;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            transition: all 0.3s ease;
        }
        .casual-button:hover {
            background-color: #f0f0f0;
            box-shadow: 0 6px 8px rgba(0, 0, 0, 0.15);
        }
        /* Center the container and set black background */
        .login-container {
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 80vh;
    
        }

        /* Card styling */
        .login-card {
            background: #000000;
            padding: 40px;
            border-radius: 15px;
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.3);
            text-align: center;
            max-width: 600px;
            width: 100%;
            transition: transform 0.3s ease;
        }

        .login-card:hover {
            transform: translateY(-5px);
        }

        /* Heading styling */
        .login-title {
            font-family: 'Segoe UI', Arial, sans-serif;
            font-size: 28px;
            color: #000000;
            margin-bottom: 15px;
            font-weight: 600;
        }

        /* Description styling */
        .login-description {
            font-family: 'Segoe UI', Arial, sans-serif;
            font-size: 16px;
            color: white;
            margin-bottom: 30px;
            line-height: 1.5;
        }

        /* Button styling */
        .login-button {
            display: inline-block;
            padding: 12px 30px;
            background: #007bff;
            
            font-family: 'Segoe UI', Arial, sans-serif;
            font-size: 16px;
            font-weight: 500;
            text-decoration: none;
            border-radius: 8px;
            transition: background 0.3s ease, transform 0.2s ease;
        }

        .login-button:hover {
            background: #0056b3;
            transform: scale(1.05);
        }

        /* Responsive design */
        @media (max-width: 600px) {
            .login-card {
                padding: 20px;
                margin: 0 15px;
            }
            .login-title {
                font-size: 24px;
            }
            .login-description {
                font-size: 14px;
            }
        }
    </style>
""", unsafe_allow_html=True)

controller = CookieController()

def encrypt(s):
    return hashlib.sha256(s.encode()).hexdigest()

try:
    if controller.get('user_token') is not None:
        # User has a token, get email
        user_token_value = controller.get('user_token')
        user_info = get_user_info(user_token_value)
        email = user_info['data']['email']

        # Validate email domain
        if re.match(r'^[a-zA-Z0-9._%+-]+@spmadridlaw\.com$', email):
            # Check if email is still active in DB
            if not is_email_active(email):
                print(f"Session expired for {email}. Logging out.")
                remove_active_user(user_token_value)
                controller.remove('user_token')
                sleep(1)
                st.rerun()

            main()
            
        else:
            remove_active_user(user_token_value)
            controller.remove('user_token')
            sleep(1)
            st.rerun()

    else:
        # Not logged in yet
        if not check_user_limit():
            st.error("Maximum number of users reached. Please try again later.")
            sleep(5)
        else:
            if 'code' in st.query_params:
                code = st.query_params.get('code')
                st.snow()
                sleep(1)
                tenant_token = get_app_access_token()
                st.snow()
                sleep(1)
                user_token = get_user_access_token(code, tenant_token)
                st.snow()
                sleep(1)
                st.query_params.clear()
                sleep(1)

                user_token_value = user_token['data']['access_token']
                user_info = get_user_info(user_token_value)
                email = user_info['data']['email']

                controller.set('user_token', user_token_value)
                add_active_user(user_token_value, email)
                sleep(1)
                st.success('LOGIN SUCCESS')
                st.rerun()

            st.markdown(
                f"""
                <div class="login-container">
                    <div class="login-card">
                        <h2 class="login-title">Authenticate with Lark Suite</h2>
                        <p class="login-description">Securely sign in to your account by clicking the button below.</p>
                       <a href="{auth_url}" style="text-decoration: none; color: black;" target="_self" class="casual-button">
                        Login with Lark Suite
                    </a>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

except Exception as e:
    st.error(e)
    if controller.get('user_token') is not None:
        remove_active_user(controller.get('user_token'))
        controller.remove('user_token')
    st.rerun()
