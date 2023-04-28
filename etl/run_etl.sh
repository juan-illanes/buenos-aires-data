#!/bin/bash

echo "1a. Creating the virtual environment..."
python3 -m venv .env
echot "... done!"

echo "1b. Activating the virtual environment..."
. .env/bin/activate
echo "... done!"

echo "1c. Installing dependencies..."
pip install --upgrade pip wheel
pip install -r requirements.txt
echo "... done!"

echo "2. Launching the ETL process defined with Luigi..."
python -m luigi --module main Load --local-scheduler
echo "... done!"

echo "3. Deactivating the virtual environment..."
deactivate
echo ".. done!"

