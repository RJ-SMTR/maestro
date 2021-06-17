FROM python:3.8-slim

# Install Docker
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN \
  apt-get update \
  && apt-get install --no-install-recommends -y apt-transport-https ca-certificates curl gnupg2 software-properties-common \
  && curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - \
  && add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" \
  && apt-get update \
  && apt-get install --no-install-recommends -y docker-ce \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy app to destination
COPY . .

# Entrypoint and command
ENTRYPOINT [ "python3" ]
CMD [ "watchdog.py" ]