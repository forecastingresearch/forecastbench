FROM ruby:3.2

WORKDIR /srv/jekyll
COPY Gemfile Gemfile.lock ./

RUN apt-get update && \
    apt-get install -y ruby-full build-essential zlib1g-dev && \
    gem install jekyll bundler minimal-mistakes-jekyll && \
    bundle install && \
    apt-get install -y apt-transport-https ca-certificates gnupg curl git openssh-client lsb-release && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    gpg --dearmor | tee /usr/share/keyrings/cloud.google.gpg >/dev/null && \
    apt-get update && \
    apt-get install -y google-cloud-sdk dumb-init

COPY . .

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
