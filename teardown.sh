#!/bin/bash

# Stop/remove containers
docker compose down

# Remove images
docker rmi apache-airflow redis postgres