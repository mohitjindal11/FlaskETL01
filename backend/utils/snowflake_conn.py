import snowflake.connector
import urllib3
import requests
import json

def get_snowflake_connection(config):
    """
    Accepts a config dict and returns a Snowflake connection object.
    Supports BASIC and OAuth authentication.
    """
    auth_type = config.get('auth_type', 'BASIC').upper()
    if auth_type == 'BASIC':
        return snowflake.connector.connect(
            user=config.get('user'),
            password=config.get('password'),
            account=config.get('account'),
            warehouse=config.get('warehouse'),
            database=config.get('database'),
            schema=config.get('schema'),
            role=config.get('role')
        )
    elif auth_type == 'OAUTH':
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'password',
            'scope': 'SESSION:ROLE-ANY',
            'username': config.get('user'),
            'password': config.get('password'),
            'account': config.get('snowflake_account'),
        }
        requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
        response = requests.post(config.get('oauth_url'), data=data, headers=headers, verify=False)
        if response.status_code != 200:
            raise Exception(response.content)
        token = str(json.loads(response.text)['access_token']).strip()
        return snowflake.connector.connect(
            user=f"{config.get('user')}@fmr.com",
            authenticator='oauth',
            token=token,
            account=config.get('account'),
            warehouse=config.get('warehouse'),
            database=config.get('database'),
            role=config.get('role')
        )
    else:
        raise Exception(f"Unknown auth_type: {auth_type}")
