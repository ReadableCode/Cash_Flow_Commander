#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Construct the file path using a relative path from the script's location
HOSTNAME=$(hostname)
FILENAME="${SCRIPT_DIR}/../triggers/crontab_extraction_${HOSTNAME}.txt"

# Extract crontab to the file
crontab -l > "$FILENAME"

# Log the completion with a timestamp
echo "$(date +"%Y-%m-%d %H:%M:%S") Completed crontab extraction"

