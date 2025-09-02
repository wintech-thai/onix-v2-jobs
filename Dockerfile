FROM ruby:3.3

RUN apt-get update -y
RUN apt-get install -y wget curl zip unzip apt-transport-https ca-certificates gnupg lsb-release 

# Install kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
RUN chmod +x kubectl
RUN mv kubectl /usr/local/bin

# Install gcloud
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN apt-get update -y && apt-get install google-cloud-cli -y
RUN gcloud -v

WORKDIR /scripts
COPY scripts/ .

RUN gem install redis elasticsearch pg google-cloud-storage
