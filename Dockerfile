FROM cassandra:latest

RUN apt-get update \
    && apt-get -y install git golang-go curl unzip \
    && cd /home

RUN curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip" \
    && unzip awscli-bundle.zip \
    && rm awscli-bundle.zip \
    && ./awscli-bundle/install -b /home/bin/aws \
    && rm -dr ./awscli-bundle

RUN mkdir /home/go \
    && export PATH=/home/bin:$PATH \
    && export GOPATH="/home/go" \
    && export GOBIN=$GOPATH/bin \
    && export PATH=$PATH:$GOBIN

RUN go get github.com/ghodss/yaml

COPY ./cassandra-backup.go "$GOPATH/src/cassandra-backup"

RUN cd /home/go/src/cassandra-backup \
    && go install

