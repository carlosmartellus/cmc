#!/bin/bash

TARGET_LINK="/usr/local/lib/cmc"
BIN_PATH="/usr/local/bin/cmc"
CURRENT_DIR=$(pwd)
REAL_USER=${SUDO_USER:-$USER}

echo -e "\033[94m[INFO]\033[0m Linking CMC Framework from: $CURRENT_DIR"

if [ ! -f "$CURRENT_DIR/pyproject.toml" ]; then
    echo -e "\033[91m[ERROR]\033[0m pyproject.toml not found."
    echo "Please run this script from the root of your CMC repository."
    exit 1
fi

echo -e "\033[94m[INFO]\033[0m Cleaning up old traces..."
sudo rm -f "$BIN_PATH"
sudo rm -rf "$TARGET_LINK"

echo -e "\033[94m[INFO]\033[0m Creating symbolic link to $TARGET_LINK..."
sudo ln -s "$CURRENT_DIR" "$TARGET_LINK"

if [ ! -d "$CURRENT_DIR/.venv" ]; then
    echo -e "\033[94m[INFO]\033[0m Creating local virtual environment..."
    python3 -m venv "$CURRENT_DIR/.venv"
fi

echo -e "\033[94m[INFO]\033[0m Synchronizing dependencies and installing in editable mode..."
"$CURRENT_DIR/.venv/bin/pip" install --upgrade pip
"$CURRENT_DIR/.venv/bin/pip" install -e "$CURRENT_DIR"

echo -e "\033[94m[INFO]\033[0m Mapping global command to $BIN_PATH..."
sudo ln -sf "$CURRENT_DIR/.venv/bin/cmc" "$BIN_PATH"
sudo chmod +x "$BIN_PATH"

sudo chown -R "$REAL_USER:$REAL_USER" "$CURRENT_DIR"

echo -e "\033[92m[SUCCESS]\033[0m CMC is now linked to your repository."
echo -e "Any changes in $CURRENT_DIR will affect the 'cmc' command globally."

echo -e "\033[94m[INFO]\033[0m Testing engines online..."
cmc --help