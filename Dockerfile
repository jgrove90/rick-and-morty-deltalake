# # *******PYTHON BASE IMAGE*******
# FROM python:3.11-slim-bullseye as python
 
# # Run installation tasks as root
# USER root

# # Project folder name
# ARG PROJECT_FOLDER=/opt/rick-and-morty-elt

# # user/group id
# ARG UID=1000
# ARG GID=1000

# # Keeps Python from generating .pyc files in the container
# ENV PYTHONDONTWRITEBYTECODE=1

# # Turns off buffering for easier container logging
# ENV PYTHONUNBUFFERED=1

# # Create user
# RUN groupadd -g "${GID}" appuser \
#   && useradd --create-home --no-log-init -u "${UID}" -g "${GID}" appuser

# # Install pip requirements
# COPY ./requirements.txt ${PROJECT_FOLDER}/requirements.txt
# RUN python -m pip install --no-cache-dir -r ${PROJECT_FOLDER}/requirements.txt

# # Copy local directory into container app directory
# WORKDIR ${PROJECT_FOLDER}
# COPY . ${PROJECT_FOLDER}

# # Set appropriate ownership for the user within the container
# RUN chown -R appuser:appuser ${PROJECT_FOLDER}

# # switch to appuser
# USER appuser

# # Start JupyterLab at container start
# CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser"]

# AIRFLOW IMAGE
FROM apache/airflow:2.6.3-python3.11 as airflow

# Project folder name
ARG PROJECT_FOLDER=/opt/rick-and-morty-elt

# user/group id
ARG UID=1000
ARG GID=1000

# Give permission to create group
USER root
RUN chmod 777 /etc/group

RUN groupadd -g "${GID}" appuser \
  && useradd --create-home --no-log-init -u "${UID}" -g "${GID}" appuser

# Copy local directory into container app directory
WORKDIR ${PROJECT_FOLDER}
COPY . ${PROJECT_FOLDER}

# Set appropriate ownership for the user within the container
RUN chown -R appuser:appuser ${PROJECT_FOLDER}

# switch to appuser
USER appuser

RUN python3.11 -m pip install --no-cache-dir -r ${PROJECT_FOLDER}/requirements.txt
