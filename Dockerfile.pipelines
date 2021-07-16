FROM python:3.8-slim

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagit and dagster-daemon, and to load the DagsterInstance

WORKDIR /tmp
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set $DAGSTER_HOME and copy dagster instance there
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

COPY .dagster_workspace/dagster.yaml $DAGSTER_HOME

# Add repository code
WORKDIR /opt/dagster/app
COPY bases/ /opt/dagster/app/bases/
COPY data/ /opt/dagster/app/data/
COPY repositories/ /opt/dagster/app/repositories/

# Run dagster gRPC server on port 4000
EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "repositories/repository.py"]
