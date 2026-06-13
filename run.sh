#!/bin/bash

# 1. Grant execution permissions to your native downloader binary
echo "Setting executable permissions on N_m3u8DL-RE..."
chmod +x binary/N_m3u8DL-RE

# 2. Start the native Tor daemon in the background
echo "Spinning up Tor infrastructure proxy..."
tor --RunAsDaemon 1 --SocksPort 9050

# 3. Give Tor a few seconds to successfully bind to port 9050
sleep 4

# 4. Fire up your Python package wrapper
echo "Launching main cantarella application framework..."
python3 -m cantarella
