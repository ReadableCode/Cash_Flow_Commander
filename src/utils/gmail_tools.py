# %%
## Running Imports ##
from __future__ import print_function
from email.message import EmailMessage

if __name__ != "__main__":
    print(f"Importing {__name__}")

import os, glob
import os.path
from time import sleep
import sys
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64
import pandas as pd
import datetime
import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
)

from utils.display_tools import print_logger, pprint_dict, pprint_ls


# %%
## Gmail Setup ##

"""
######### Features #########

Without force download, each email will be downloaded only once, its message id is logged in a file after the first successful download

######### Usage #########

--- creating credentials ---
Open the Google Cloud Console @ https://console.cloud.google.com/
At the top-left, click Menu menu > APIs & Services > Credentials.
Click Create Credentials > oauth client ID.
Click Application type > Desktop app.
In the "Name" field, type a name for the credential. This name is only shown in the Cloud Console.
Click Create. The oauth client created screen appears, showing your new Client ID and Client secret.
Click OK. The newly created credential appears under "oauth 2.0 Client IDs."
Save in an auth_dir specefic to the account you are using
on the app screen go to oauth consent screen, next until you hit scopes, and add gmail.compose and or gmail.readonly depending on scope of project

--- downloading existing credentials ---
Open the Google Cloud Console @ https://console.cloud.google.com/
At the top-left, click Menu menu > APIs & Services > Credentials.
Click oauth client ID > Select the client ID you created.
Click Download JSON.
Save in an auth_dir specefic to the account you are using
on the app screen go to oauth consent screen, next until you hit scopes, and add gmail.compose and or gmail.readonly depending on scope of project

--- to import ---
from utils.gmail_tools import get_attachment_from_search_string

---search_string---
set gmail search string, can use todays date but must be a string or f string

---output_path---
use os.path.join on your path to make sure it is correct

---output_file_name---
set output_file_name to None if you want to use the original file name
set output_file_name to 'original with date' if you want to use the original file name with date

---force_download---
set force_download to True if you want to download the file even if it has already been downloaded

---retries---
Set retry variable if inconsistant results, default is 3, will not duplicate files or keep trying if it succeeds

######### Limitations #########

Only downloads first file in the attachments of each email

"""


# %%
## Authentication ##

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def get_gmail_service(auth_dir):
    token_path = os.path.join(auth_dir, "token.json")
    oauth_path = os.path.join(auth_dir, "oauth.json")

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(
            token_path,
            SCOPES,
        )

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                oauth_path,
                SCOPES,
            )
            creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open(
                token_path,
                "w",
            ) as token:
                token.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds)

    return service


# %%
## Sending Email ##


def send_message(service, user_id, message):
    try:
        message = (
            service.users().messages().send(userId=user_id, body=message).execute()
        )
        print("Message sent successfully!")
        return message
    except Exception as e:
        print("An error occurred while sending the message:", str(e))


def create_attachment(attachment_path):
    attachment_filename = os.path.basename(attachment_path)
    mime_type, _ = mimetypes.guess_type(attachment_path)
    mime_type, mime_subtype = mime_type.split("/", 1)

    with open(attachment_path, "rb") as file:
        attachment = MIMEBase(mime_type, mime_subtype)
        attachment.set_payload(file.read())

    encoders.encode_base64(attachment)
    attachment.add_header(
        "Content-Disposition", "attachment", filename=attachment_filename
    )
    return attachment


def create_message(
    sender_name, sender_email, to, subject, message_text, attachment_path=None
):
    message = MIMEMultipart()
    message["to"] = to
    message["from"] = formataddr((sender_name, sender_email))
    message["subject"] = subject

    message.attach(MIMEText(message_text, "html"))  # Set the MIME type to HTML

    if attachment_path:
        attachment = create_attachment(attachment_path)
        message.attach(attachment)

    return {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode()}


def send_email(
    sender_name,
    sender_email,
    to,
    subject,
    message_text,
    auth_dir=os.path.join(
        great_grandparent_dir, "credentials", "personal", "gmail_auth"
    ),
    attachment_path=None,
):
    service = get_gmail_service(auth_dir)
    message = create_message(
        sender_name, sender_email, to, subject, message_text, attachment_path
    )
    send_message(service, "me", message)


# %%
## Funtions ##


def get_attachment_from_search_string(
    search_string,
    output_path,
    output_file_name=None,
    force_download=False,
    auth_dir=os.path.join(
        great_grandparent_dir, "credentials", "personal", "gmail_auth"
    ),
):
    # search gmail for message
    service = get_gmail_service(auth_dir=auth_dir)
    results = service.users().messages().list(userId="me", q=search_string).execute()
    all_messages_from_search = results.get("messages", [])
    ls_paths_with_file_names = []
    if not all_messages_from_search:
        print("No messages found.")
        return
    for this_message_from_reults in all_messages_from_search:
        message_id = this_message_from_reults["id"]
        print("message_id: " + str(message_id))

        message_was_already_done = False

        if not os.path.exists(os.path.join(file_dir, "done_message_ids.txt")):
            with open(os.path.join(file_dir, "done_message_ids.txt"), "w") as f:
                f.write("")

        else:
            with open(
                os.path.join(file_dir, "done_message_ids.txt"), "r+"
            ) as message_log:
                for line in message_log:
                    if line.strip() == str(message_id):
                        message_was_already_done = True
                        continue

        if message_was_already_done and not force_download:
            print("Message already downloaded and force_download is False")
            continue
        elif message_was_already_done and force_download:
            print(
                "Message already downloaded and force_download is True, downloading again"
            )

        executed_message = (
            service.users().messages().get(userId="me", id=message_id).execute()
        )

        internal_date_received = executed_message["internalDate"]

        internal_dt_received = pd.to_datetime(
            int(float(internal_date_received) / 1000), unit="s", origin="unix"
        ).strftime(format="%Y-%m-%d")
        str_internal_dt_received = str(internal_dt_received)

        internal_dt_received_with_seconds = pd.to_datetime(
            int(float(internal_date_received) / 1000), unit="s", origin="unix"
        ).strftime(format="%Y.%m.%d %H.%M.%S")
        str_internal_dt_received_with_seconds = str(internal_dt_received_with_seconds)

        message_subject = ""
        for item in executed_message["payload"]["headers"]:
            if item["name"] == "Subject":
                message_subject = item["value"]
                print(f"subject is {message_subject}")
                break
        for part in executed_message["payload"]["parts"]:
            if part["filename"]:
                print("detected attachment type 1")
                original_file_extension = part["filename"].split(".")[-1]
                if output_file_name == None:
                    print("output_file_name is None, using original file name")
                    filename = part["filename"]
                    filename = (
                        filename.replace("/", " ")
                        .replace(":", "")
                        .replace("?", "")
                        .replace("&", "and")
                    )
                elif output_file_name == "original with date":
                    print(
                        "output_file_name is original with date, using original file name with date"
                    )
                    filename = (
                        part["filename"]
                        + " "
                        + str_internal_dt_received
                        + "."
                        + original_file_extension
                    )
                    filename = (
                        filename.replace("/", " ")
                        .replace(":", "")
                        .replace("?", "")
                        .replace("&", "and")
                    )
                elif output_file_name == "original with date and seconds":
                    print(
                        "output_file_name is original with date and seconds, using original file name with date and seconds"
                    )
                    filename = (
                        part["filename"]
                        + " "
                        + str_internal_dt_received_with_seconds
                        + "."
                        + original_file_extension
                    )
                    filename = (
                        filename.replace("/", " ")
                        .replace(":", "")
                        .replace("?", "")
                        .replace("&", "and")
                    )
                else:
                    filename = output_file_name
                    filename = (
                        filename.replace("/", " ")
                        .replace(":", "")
                        .replace("?", "")
                        .replace("&", "and")
                    )
                print(f"filename is {filename}")
                attachment = (
                    service.users()
                    .messages()
                    .attachments()
                    .get(
                        id=part["body"]["attachmentId"],
                        userId="me",
                        messageId=message_id,
                    )
                    .execute()
                )
                file_data = base64.urlsafe_b64decode(attachment["data"].encode("utf-8"))
                break
            else:
                try:
                    if part["parts"][0]["filename"]:
                        print("detected attachment type 2")
                        original_file_name = part["parts"][0]["filename"]
                        # replace characters for windows
                        original_file_name = (
                            original_file_name.replace(":", " -")
                            .replace("/", "-")
                            .replace("\\", "-")
                            .replace("&", "-")
                        )
                        original_file_name = (
                            original_file_name.replace("/", " ")
                            .replace(":", "")
                            .replace("?", "")
                            .replace("&", "and")
                        )
                        original_file_extension = original_file_name.split(".")[-1]
                        if output_file_name == "original with subject and datetime":
                            filename = (
                                original_file_name
                                + " "
                                + message_subject
                                + str_internal_dt_received_with_seconds
                                + "."
                                + original_file_extension
                            )
                        elif output_file_name == "domo split":
                            filename = (
                                original_file_name.split(" - ")[0]
                                + " - Active Roster - "
                                + str_internal_dt_received
                                + " - "
                                + original_file_name.replace("|||", "$").split("$")[1]
                                + "."
                                + original_file_extension
                            )
                        elif output_file_name == "highjump":
                            filename = (
                                "highjump file"
                                + " "
                                + str_internal_dt_received_with_seconds
                                + "."
                                + original_file_extension
                            )
                        attachment_id = part["parts"][0]["body"]["attachmentId"]
                        attachment = (
                            service.users()
                            .messages()
                            .attachments()
                            .get(id=attachment_id, userId="me", messageId=message_id)
                            .execute()
                        )
                        file_data = base64.urlsafe_b64decode(
                            attachment["data"].encode("utf-8")
                        )
                        break
                except:
                    print("did not detect attachment type 2")

        path_with_file_name = os.path.join(output_path, filename)
        print(f"path_with_file_name is {path_with_file_name}")
        with open(path_with_file_name, "wb") as f:
            f.write(file_data)
        ls_paths_with_file_names.append(path_with_file_name)

        if not force_download:
            with open(
                os.path.join(file_dir, "done_message_ids.txt"), "a+"
            ) as message_log:
                message_log.write(str(message_id) + "\n")
        else:
            print("force download so not logging message id")

    return ls_paths_with_file_names


# %%
## email addresses ##


def get_email_addresses_from_search_string(
    search_string,
    auth_dir=os.path.join(
        great_grandparent_dir, "credentials", "personal", "gmail_auth"
    ),
):
    # search gmail for message
    service = get_gmail_service(auth_dir=auth_dir)
    results = service.users().messages().list(userId="me", q=search_string).execute()
    all_messages_from_search = results.get("messages", [])
    print(f"found {len(all_messages_from_search)} messages")

    if not all_messages_from_search:
        print("No messages found.")
        return

    email_addresses = []
    email_regex = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

    for this_message_from_results in all_messages_from_search:
        message_id = this_message_from_results["id"]

        executed_message = (
            service.users().messages().get(userId="me", id=message_id).execute()
        )

        if "parts" in executed_message["payload"]:
            for part in executed_message["payload"]["parts"]:
                print(part["mimeType"])
                print("#" * 30)
                if part["mimeType"] == "text/plain":
                    decoded_data = base64.urlsafe_b64decode(
                        part["body"]["data"].encode("utf-8")
                    ).decode("utf-8")
                    print(decoded_data)
                    email_matches = re.findall(email_regex, decoded_data)
                    email_addresses.extend(email_matches)
                elif part["mimeType"] == "text/html":
                    decoded_data = base64.urlsafe_b64decode(
                        part["body"]["data"].encode("utf-8")
                    ).decode("utf-8")
                    print(decoded_data)
                    email_matches = re.findall(email_regex, decoded_data)
                    email_addresses.extend(email_matches)

    return list(set(email_addresses))


# %%
## If Main Then Do Some Testing ##


auth_dir = os.path.join(great_grandparent_dir, "credentials", "personal", "gmail_auth")


if __name__ == "__main__":
    ############################### Download Roster Files ##############################
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")
    gmail_search_string = f"Job change reversion for Lyons"
    print(f" ############ Searching for: {gmail_search_string} ############")

    email_addresses = get_email_addresses_from_search_string(
        gmail_search_string,
    )
    print(email_addresses)


# %%
## If Main Then Do Some Testing ##

if __name__ == "__main__":
    ############################### Download Roster Files ##############################
    today_date = datetime.datetime.now().strftime("%Y-%m-%d")
    gmail_search_string = f"Fwd: US PSP Fees March 2023 after:{today_date}"
    print(f" ############ Searching for: {gmail_search_string} ############")

    get_attachment_from_search_string(
        gmail_search_string,
        data_dir,
        force_download=True,
    )


# %%
