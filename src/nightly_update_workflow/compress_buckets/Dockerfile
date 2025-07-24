FROM us-central1-docker.pkg.dev/serverless-runtimes/google-22-full/runtimes/python312

RUN whoami || echo "whoami not found"

USER root

RUN apt-get update && \
    apt-get install -y apt-transport-https ca-certificates gnupg curl git openssh-client lsb-release && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk dumb-init -y

USER www-data

COPY . /app

WORKDIR /app

RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]
