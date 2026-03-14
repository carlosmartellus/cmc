#!/bin/bash

set -euo pipefail
shopt -s failglob

REPO_URL="https://github.com/carlosmartellus/cmc.git"

TARGET_DIR="${1:-/usr/local/lib/cmc}"
BIN_PATH="${2:-/usr/local/bin/cmc}"

echo "Installing CMC from GitHub"

if [ -d "$TARGET_DIR" ]; then
    sudo rm -rf "${TARGET_DIR:?Error: TARGET_DIR no definida}"
else
    exit 1
fi

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