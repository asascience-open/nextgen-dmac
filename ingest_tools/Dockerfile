FROM python:3.10-bullseye

RUN mkdir -p /opt/ingest_tools
WORKDIR /opt/ingest_tools

# Install native dependencies
RUN apt-get -y update && apt-get install -y \
    libudunits2-dev \
    libgdal-dev \
    libnetcdf-dev \
    libeccodes-dev

# Install python dependencies
COPY ./requirements.txt ./requirements.txt
RUN python3 -m pip config set global.http.sslVerify false
RUN git config --global http.sslverify false

COPY ./ingest_tools ./ingest_tools
COPY ./pyproject.toml ./pyproject.toml

RUN pip3 install .