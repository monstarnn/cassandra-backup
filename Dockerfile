FROM cassandra:latest

RUN apt-get update \
    && apt-get -y install git golang-go curl unzip wget \
    && cd /home

RUN curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip" \
    && unzip awscli-bundle.zip \
    && rm awscli-bundle.zip \
    && ./awscli-bundle/install -b /home/bin/aws \
    && rm -dr ./awscli-bundle

ENV PATH="/home/bin:$PATH"
ENV GOPATH="/home/go"
ENV GOBIN="$GOPATH/bin"
ENV PATH="$PATH:$GOBIN"

RUN mkdir -p $GOPATH/src/cassandra-backup
RUN go get github.com/ghodss/yaml

COPY ./cassandra-backup.go "$GOPATH/src/cassandra-backup/"

RUN cd /home/go/src/cassandra-backup \
    && go install

#RUN sed -i "/exec \"/d" /docker-entrypoint.sh \
#    && echo "echo $PATH" >> /docker-entrypoint.sh \
#    && echo "cassandra-backup start" >> /docker-entrypoint.sh \
#    && echo "echo '!!!!!!!!!!!!'" >> /docker-entrypoint.sh \
#    && echo "whoami" >> /docker-entrypoint.sh \
#    && echo "echo '!!!!!!!!!!!!'" >> /docker-entrypoint.sh \
#    && echo 'exec "$@"' >> /docker-entrypoint.sh

CMD ["cassandra", "-f"]
