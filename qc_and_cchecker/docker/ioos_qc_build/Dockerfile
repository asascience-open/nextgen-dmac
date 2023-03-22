FROM python:3.11

RUN apt-get update && apt-get -y install libhdf5-dev && \
    rm -rf /var/lib/apt/lists/* && pip install --no-cache ioos_qc xarray
COPY ioos_qc_script.py /
