#!/bin/bash

REPO_URL="https://github.com/carlosmartellus/cmc"
INSTALL_DIR="$HOME/cmc"
TARGET_LINK="/usr/local/lib/cmc"
BIN_PATH="/usr/local/bin/cmc"
REAL_USER=${SUDO_USER:-$USER}

echo -e "\033[94m[INFO]\033[0m Forging CMC Framework"

echo -e "\033[94m[INFO]\033[0m Checking system dependencies..."
SYSTEM_DEPS=("git" "python3" "python3-venv" "python3-dev" "libpq-dev" "build-essential")

sudo apt-get update -y
for dep in "${SYSTEM_DEPS[@]}"; do
    if dpkg -s "$dep" >/dev/null 2>&1; then
        echo -e "\033[92m[OK]\033[0m $dep is already installed."
    else
        echo -e "\033[93m[MISSING]\033[0m Installing $dep..."
        sudo apt-get install -y "$dep"
    fi
done

if [ ! -f "pyproject.toml" ]; then
    echo -e "\033[94m[INFO]\033[0m No CMC source detected. Cloning from GitHub..."
    if [ -d "$INSTALL_DIR" ]; then
        sudo rm -rf "$INSTALL_DIR"
    fi
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
else
    echo -e "\033[92m[OK]\033[0m Running from local source."
    INSTALL_DIR=$(pwd)
fi

echo -e "\033[94m[INFO]\033[0m Cleaning up old traces..."
sudo rm -f "$BIN_PATH"
sudo rm -rf "$TARGET_LINK"

echo -e "\033[94m[INFO]\033[0m Linking core to $TARGET_LINK..."
sudo ln -s "$INSTALL_DIR" "$TARGET_LINK"

if [ ! -d ".venv" ]; then
    echo -e "\033[94m[INFO]\033[0m Creating virtual environment..."
    python3 -m venv .venv
fi

echo -e "\033[94m[INFO]\033[0m Installing CMC in editable mode..."
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e .

echo -e "\033[94m[INFO]\033[0m Mapping 'cmc' command globally..."
sudo ln -sf "$INSTALL_DIR/.venv/bin/cmc" "$BIN_PATH"
sudo chmod +x "$BIN_PATH"

sudo chown -R "$REAL_USER:$REAL_USER" "$INSTALL_DIR"

echo -e "\033[94m[INFO]\033[0m Activating bash autocomplete..."
if ! grep -q "register-python-argcomplete cmc" "$HOME/.bashrc"; then
    echo 'eval "$('"$INSTALL_DIR"'/.venv/bin/register-python-argcomplete cmc)"' >> "$HOME/.bashrc"
    echo -e "\033[93m[NOTE]\033[0m Autocomplete installed. Please run 'source ~/.bashrc' or restart your terminal."
fi

echo -e "\033[92m[SUCCESS]\033[0m CMC Framework is ready to roll."
echo -e "\033[94m[INFO]\033[0m Source location: $INSTALL_DIR"
echo -e "\033[94m[INFO]\033[0m Global command: cmc"

cmc --help