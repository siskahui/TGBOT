#!/bin/bash
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
TARGET_DIR="backups"
mkdir -p "TARGET_DIR"
cp tg.py "$TARGET_DIR/tg_$TIMESTAMP.py"
echo "Копия создана: $TARGET_DIR/tg_$TIMESTAMP.py"
