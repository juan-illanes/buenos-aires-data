#!/bin/bash

echo "0a. Building the postgres image..."
docker build -t oltp .
echo "... done!"

echo "0b. Starting the postgres container..."
docker run --name oltp -e POSTGRES_PASSWORD=should_be_a_secret -p 5432:5432 -d oltp
echo "... done!"

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

echo "2. Processing the CSV file and populating the DB..."
python oltp.py
echo "... done!"

echo "3. Deactivating the virtual environment..."
deactivate
echo ".. done!"
