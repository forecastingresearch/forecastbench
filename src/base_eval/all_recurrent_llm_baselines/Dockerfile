FROM gcr.io/deeplearning-platform-release/base-cpu

# Define build-time variables
ARG FORECAST_SETS_BUCKET
ARG QUESTION_SETS_BUCKET
ARG LLM_BASELINE_NEWS_BUCKET
ARG CLOUD_PROJECT

# Set environment variables from build-time arguments
ENV FORECAST_SETS_BUCKET=${FORECAST_SETS_BUCKET}
ENV QUESTION_SETS_BUCKET=${QUESTION_SETS_BUCKET}
ENV LLM_BASELINE_NEWS_BUCKET=${LLM_BASELINE_NEWS_BUCKET}
ENV CLOUD_PROJECT=${CLOUD_PROJECT}

# Install dependencies for building Python
RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libncurses5-dev \
    libncursesw5-dev \
    libreadline-dev \
    libsqlite3-dev \
    libgdbm-dev \
    libdb5.3-dev \
    libbz2-dev \
    libexpat1-dev \
    liblzma-dev \
    tk-dev \
    libffi-dev \
    libnss3-dev \
    libgdbm-compat-dev \
    && rm -rf /var/lib/apt/lists/*

# Download and install Python 3.12.4 because no vertex ai's container image has python 3.12
RUN wget https://www.python.org/ftp/python/3.12.4/Python-3.12.4.tgz \
    && tar xzf Python-3.12.4.tgz \
    && cd Python-3.12.4 \
    && ./configure --enable-optimizations \
    && make altinstall \
    && cd .. \
    && rm -rf Python-3.12.4 Python-3.12.4.tgz

# Ensure pip is up-to-date
RUN python3.12 -m ensurepip
RUN python3.12 -m pip install --upgrade pip

# Copy requirements and install them
COPY requirements.txt .
RUN python3.12 -m pip install -r requirements.txt


# Install and register the Python kernel
RUN python3.12 -m ipykernel install --name python3 --display-name "Python 3.12"

# Copy the application code
COPY . /app
WORKDIR /app

# Set the entrypoint to use the specific Python version
ENTRYPOINT ["python3.12", "run_eval.py"]