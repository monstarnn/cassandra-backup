# Cassandra-backup

#### Start

Create docker
```
docker build -t cassandra-backup .
```

Run docker
```
docker run --rm -e AWS_ACCESS_KEY_ID="AAAAAAA" -e AWS_SECRET_ACCESS_KEY="BBBBBBB" \
  -e AWS_DEFAULT_REGION="CCCCCCCC" -e AWS_BUCKET="DDDDDDD" --name cassandra-backup cassandra-backup
```

Connect node
```
docker exec -it cassandra-backup bash
```

#### Tune node

Restore last backup
```
cassandra-backup restore
```

Make single backup
```
cassandra-backup single
```

Start backups job (every 3600 seconds, default value if not set 86400)
```
cassandra-backup start 3600
```

#### Problem

Cassandra starts by `/docker-entrypoint.sh` from user `cassandra`, but `cassandra-backup` should start from `root`.

*TODO:* start `cassandra -f` from `cassandra` and `cassandra-backup` from `root` both.