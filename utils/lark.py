import requests
import json
from dotenv import load_dotenv
import os


current_working_dir = os.getcwd()
# env_path = os.path.join(current_working_dir, 'config', '.env')
env_path = "/home/ubuntu/bcp/config/.env"
load_dotenv(dotenv_path=env_path)

APP_ID = os.getenv('lark_app_id')
APP_SECRET = os.getenv('lark_app_secret')
REDIRECT_URI = os.getenv('lark_redirect_uri')


def lark_req(url, headers, payload, method="POST"):
    return requests.request(method, url, headers=headers, data=json.dumps(payload)).json()

def get_app_access_token():
    return lark_req(
        url="https://open.larksuite.com/open-apis/auth/v3/app_access_token/internal",
        payload={ "app_id": f"{APP_ID}", "app_secret": f"{APP_SECRET}" },
        headers={ 'Content-Type': 'application/json' }
    )['tenant_access_token']

def get_user_access_token(code, tenant_access_token):
    return lark_req(
        url="https://open.larksuite.com/open-apis/authen/v1/oidc/access_token",
        payload={ "code": code, "grant_type": "authorization_code"},
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {tenant_access_token}'
        }
    )

def get_user_info(user_access_token):
    return lark_req(
        url="https://open.larksuite.com/open-apis/authen/v1/user_info",
        payload='',
        headers={ 'Authorization': f'Bearer {user_access_token}' },
        method='GET'
    )