#!/bin/bash

# ***AIRFLOW FOLDERS
# Create airflow folder
airflow_dir="./airflow"
mkdir -p "${airflow_dir}"

# Create subfolders
for folder in "logs" "dags" "plugins" "config"; do
    subfolder="${airflow_dir}/${folder}"
    mkdir -p "${subfolder}"
done

echo " - Airflow folder structure created successfully!"

# ****DELTALAKE FODLERS
# Create Deltalake folder
deltalake_dir="./deltalake"
mkdir -p "${deltalake_dir}"

# Create Rick and Morty folder
rick_morty_dir="${deltalake_dir}/rick_and_morty"
mkdir -p "${rick_morty_dir}"

# Create subfolders inside Rick and Morty folder
for folder in "bronze" "silver" "gold"; do
    subfolder="${rick_morty_dir}/${folder}"
    mkdir -p "${subfolder}"

    # Create character, episode, and location folders inside subfolder
    for item in "character" "episode" "location"; do
        item_folder="${subfolder}/${item}"
        mkdir -p "${item_folder}"
    done

    # Create fact folder inside gold subfolder
    if [ "${folder}" = "gold" ]; then
        fact_folder="${subfolder}/fact"
        mkdir -p "${fact_folder}"
    fi
done

echo " - Deltalake folder structure created successfully!"

# ***CSV DATA FOLDER
# Create csv_data folder
csv_data="./csv_data"
mkdir -p "${csv_data}"

echo " - CSV Data folder structure created successfully!"

# *** .ENV FILE
# Create env file
echo "AIRFLOW_UID=$(id -u)" > .env

echo " - .env created successfully!"

# ***DOCKER BUILD
docker build --target python -t rick-and-morty-deltalake . && \
docker build --target airflow -t apache-airflow . 