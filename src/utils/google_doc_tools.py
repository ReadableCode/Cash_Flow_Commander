# %%
# Imports #

import json
import os
import sys

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# append grandparent
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import grandparent_dir
from utils.display_tools import pprint_dict, print_logger

# %%
# Load Environment #

# source .env file
dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


# %%
# Google Credentials #

service_account_env_key = "GOOGLE_SERVICE_ACCOUNT"
service_account_email = json.loads(os.getenv(service_account_env_key))["client_email"]
print_logger(f"google_service_account email: {service_account_email}")


if os.getenv(service_account_env_key):
    print_logger("Using service account credentials from environment variable")
    # create credentials from google service account info
    credentials_docs = service_account.Credentials.from_service_account_info(
        json.loads(os.getenv(service_account_env_key), strict=False),
        scopes=["https://www.googleapis.com/auth/documents"],
    )
else:
    print_logger("Using json file for service account credentials")
    # create credentials from json file
    credentials_docs = service_account.Credentials.from_service_account_file(
        os.path.join(grandparent_dir, "service_account_credentials.json"),
        scopes=["https://www.googleapis.com/auth/documents"],
    )

# Create a Google Docs API client
docs_service = build("docs", "v1", credentials=credentials_docs)


# %%
# Google Docs Functions #


def get_google_doc_from_id(id):
    """
    Get a Google Doc object by its ID.
    Args:
        id (str): The ID of the Google Doc.

    Returns:
        dict: The Google Doc object, or None if an error occurred.
    """

    try:
        document = docs_service.documents().get(documentId=id).execute()
        return document
    except HttpError as e:
        print_logger(f"Error: {e}", level="warning")
        return None


def print_contents_of_doc_by_id(id):
    """
    Print the contents of a Google Doc by its ID.
    Args:
        id (str): The ID of the Google Doc.
    """
    document = get_google_doc_from_id(id)
    if document is None:
        return
    pprint_dict(document["body"]["content"])
    return document["body"]["content"]


def append_text_to_doc_by_id(id, text):
    """
    Append text to a Google Doc by its ID.
    Args:
        id (str): The ID of the Google Doc.
        text (str): The text to append.

    Returns:
        dict: The result of the batch update operation.
    """

    # Get the current length of the document
    document = docs_service.documents().get(documentId=id).execute()
    current_length = document["body"]["content"][-1]["endIndex"] - 1

    requests = [
        {
            "insertText": {
                "location": {"index": current_length},
                "text": text,
            }
        }
    ]
    result = (
        docs_service.documents()
        .batchUpdate(documentId=id, body={"requests": requests})
        .execute()
    )
    return result


# %%
