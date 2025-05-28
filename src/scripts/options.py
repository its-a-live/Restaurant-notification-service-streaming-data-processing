kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"manager\" password=\"******\";',
    'kafka.bootstrap.servers': '?cloud.net:9091',
}
psql_settings_for_docker = {
    'user': 'admin',
    'password': 'admin',
    'url': f'jdbc:postgresql://localhost:5432/postgres',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.create_subscribers_feedback',
}

psql_settings = {
    'user': 'admin',
    'password': 'admin',
    'url': f'jdbc:postgresql://?cloud.net:6432/',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'subscribers_restaurants',
}

spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

TOPIC_IN = 'source.in'
TOPIC_OUT = 'storage.out'
