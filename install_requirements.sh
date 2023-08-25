#!/bin/bash

# Check if a virtual environment already exists
if [ ! -d "venv" ]; then
    # Create a virtual environment
    python3 -m venv venv
fi

# Activate the virtual environment
source venv/bin/activate

# Install the libraries from requirements.txt
pip install --no-cache-dir -r requirements.txt

# Deactivate the virtual environment
deactivate