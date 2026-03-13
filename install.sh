#!/bin/bash

REPO_URL="https://github.com/carlosmartellus/cmc.git"
TARGET_DIR="/usr/local/lib/cmc"
BIN_PATH="/usr/local/bin/cmc"

echo "Installing CMC from GitHub"

sudo rm -rf "$TARGET_DIR"

sudo git clone "$REPO_URL" "$TARGET_DIR"

echo "Configuring virtual environment"
sudo python3 -m venv "$TARGET_DIR/.venv"

echo "Installing dependencies"
sudo "$TARGET_DIR/.venv/bin/pip" install -r "$TARGET_DIR/requirements.txt"

cat << EOW > cmc_wrapper
#!/bin/bash
$TARGET_DIR/.venv/bin/python3 $TARGET_DIR/cmc.py "\$@"
EOW

sudo mv cmc_wrapper "$BIN_PATH"
sudo chmod +x "$BIN_PATH"

echo "CMC is ready to use"