# %%
## Google ##

import os
import sys
import json
import requests


# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
)


# %%
## Send Messages ##


def send_slack_message(webhook_url, text, attachment=None):
    """
    Send a message to a Slack channel using a webhook.

    Args:
    webhook_url (str): The Slack webhook URL.
    text (str): The message text to send.
    attachment (dict, optional): A dictionary representing a Slack attachment. Defaults to None.
    """
    payload = {
        "text": text,
    }

    if attachment:
        payload["attachments"] = [attachment]

    headers = {"Content-Type": "application/json"}

    response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)

    if response.status_code != 200:
        raise ValueError(
            f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}"
        )


def send_slack_message_with_bot_token(webhook_url, text, channel, file_path=None):
    """
    Send a message to a Slack channel using a webhook and Slack API for file uploads.

    Args:
    webhook_url (str): The Slack webhook URL.
    text (str): The message text to send.
    channel (str): The Slack channel ID to send the message to.
    file_path (str, optional): A file path to upload as an attachment. Defaults to None.
    """
    # Send the text message using the webhook
    payload = {
        "text": text,
    }

    headers = {"Content-Type": "application/json"}

    response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)

    if response.status_code != 200:
        raise ValueError(
            f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}"
        )

    # Upload the file using the Slack API if a file_path is provided
    if file_path:
        api_url = "https://slack.com/api/files.upload"
        headers = {
            "Authorization": f"Bearer SLACK_BOT_TOKEN",
            "Content-Type": "multipart/form-data",
        }

        with open(file_path, "rb") as file:
            response = requests.post(
                api_url,
                headers=headers,
                data={"channels": channel},
                files={"file": file},
            )

        response_json = response.json()

        if not response_json["ok"]:
            raise ValueError(
                f"File upload to Slack failed, the response is:\n{response.text}"
            )


# %%
## Send Messages With Webhook ##

if __name__ == "__main__":
    webhook_url = "https://hooks.slack.com/services/your/webhook/url"

    # Example text message
    text = "Hello, this is a message from my Python script!"

    # Example attachment (optional)
    attachment = {
        "fallback": "This is a fallback text for the attachment",
        "color": "#36a64f",
        "pretext": "This is an optional attachment pretext",
        "title": "Attachment Title",
        "title_link": "https://example.com",
        "text": "Attachment text",
    }

    send_slack_message(webhook_url, text, attachment)


# %%
## Send Messages With Bot Token ##

if __name__ == "__main__":
    webhook_url = "https://hooks.slack.com/services/your/webhook/url"

    # Example text message
    text = "Hello, this is a message from my Python script!"

    # Example Slack channel ID
    channel = "C12345678"

    # Example file path for attachment (optional)
    file_path = "/path/to/your/file.txt"

    send_slack_message(webhook_url, text, channel, file_path)


# %%
