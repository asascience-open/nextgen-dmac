FROM nextgen-dmac/ingest-tools

# Create the runtime location for the service to run at
RUN mkdir -p /function

# Install the function's dependencies
RUN pip install --target /function awslambdaric

# Move to the function dir
WORKDIR /function

# Copy the function code
COPY ./aggregation.py ./aggregation.py

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
# Pass the name of the function handler as an argument to the runtime
CMD [ "aggregation.handler" ]