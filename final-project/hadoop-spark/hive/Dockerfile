FROM bde2020/hive:2.3.2-postgresql-metastore

COPY hive/init-hive.sh /docker-entrypoint-initdb.d/init-hive.sh
COPY hive/hive-schema.hql /hive/hive-schema.hql

RUN chmod +x /docker-entrypoint-initdb.d/init-hive.sh
