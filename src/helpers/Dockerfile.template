FROM REGION-docker.pkg.dev/serverless-runtimes/google-22-full/runtimes/python312

RUN whoami || echo "whoami not found"

USER root
RUN apt-get update && apt-get install -y git openssh-client
USER www-data

COPY . /app

WORKDIR /app

RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]
