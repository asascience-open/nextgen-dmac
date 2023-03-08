FROM python:3.11-bullseye

RUN mkdir -p /opt/kerchunk
WORKDIR /opt/kerchunk

# Install native dependencies
RUN apt-get -y update && apt-get install -y \
    libudunits2-dev \
    libgdal20 \
    libnetcdf-dev \
    libeccodes-dev

# Install python dependencies
COPY ./requirements.txt ./requirements.txt
RUN python3 -m pip config set global.http.sslVerify false
RUN git config --global http.sslverify false
RUN pip3 install -r requirements.txt