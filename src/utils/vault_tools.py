# %%
## Imports ##

import hvac
import requests
import os
import sys
from dotenv import load_dotenv

from config_utils import grandparent_dir, parent_dir

# %%
## Variables ##


dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

user_env_path = os.path.join(grandparent_dir, "user.env")
if os.path.exists(user_env_path):
    load_dotenv(user_env_path)

vault_url = os.getenv("VAULT_URL")
vault_namespace = os.getenv("VAULT_NAMESPACE")
vault_token = os.getenv("VAULT_TOKEN")

# Initialize the Vault client
client = hvac.Client(
    url=vault_url,  # Replace with your Vault server URL
    namespace=vault_namespace,
    token=vault_token,  # Replace with your initial Vault token
)


# %%
## Vault Functions ##


def list_vault_mounts():
    api_endpoint = f"{vault_url}/v1/sys/mounts"

    headers = {
        "X-Vault-Token": vault_token,
        "X-Vault-Namespace": vault_namespace,
    }

    response = requests.get(api_endpoint, headers=headers)

    if response.status_code == 200:
        data = response.json()
        mounts = data["data"]

        print("Enabled Secret Engines and Mount Points:")
        for mount_point, mount_config in mounts.items():
            print(f"{mount_point}: {mount_config['type']}")

        return mounts

    else:
        print(f"Failed to list mounts. Status code: {response.status_code}")


def list_secrets(mount_point, secret_path):
    try:
        response = client.secrets.kv.v2.read_secret_version(
            path=secret_path, mount_point=mount_point
        )

        if "data" in response:
            data = response["data"]
            print("Successfully read secrets:")
            for key, value in data["data"].items():
                print(f"key: {key}")
            return data["data"]
        else:
            print(f"Failed to read secret. Status code: {response.status_code}")
    except Exception as e:
        print(f"Connection to Vault failed: {e}")


def get_vault_secret(mount_point, secret_path, secret_key):
    try:
        response = client.secrets.kv.v2.read_secret_version(
            path=secret_path, mount_point=mount_point
        )

        if "data" in response:
            data = response["data"]
            if secret_key in data["data"]:
                return data["data"][secret_key]
            else:
                print(f"Secret key {secret_key} not found.")
        else:
            print(f"Failed to read secret. Status code: {response.status_code}")
    except Exception as e:
        print(f"Connection to Vault failed: {e}")


# %%
## Main ##

if __name__ == "__main__":
    list_vault_mounts()
    list_secrets("staging/key-value", "test")

    mount_point = "staging/key-value"
    secret_path = "test"
    secret_key = "foo"
    secret = get_vault_secret(mount_point, secret_path, secret_key)
    print(secret)

    mount_point = "common/key-value"
    secret_path = "photon"
    secret_key = "na_finops_service_token"
    secret = get_vault_secret(mount_point, secret_path, secret_key)
    print(secret)


# %%
