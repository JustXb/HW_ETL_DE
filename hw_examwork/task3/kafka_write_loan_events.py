from __future__ import annotations

import argparse
import sys
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="JSONL file in Object Storage")
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--username", required=True, help="Kafka username")
    parser.add_argument("--password", required=True, help="Kafka password")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(
        "Kafka writer args: "
        f"input={args.input}, bootstrap_servers={args.bootstrap_servers}, "
        f"topic={args.topic}, username={args.username}",
        flush=True,
    )
    spark = SparkSession.builder.appName("loan-events-kafka-write").getOrCreate()

    events = spark.read.text(args.input).where(col("value").isNotNull()).coalesce(4)

    events.write.format("kafka").option(
        "kafka.bootstrap.servers",
        args.bootstrap_servers,
    ).option(
        "topic",
        args.topic,
    ).option(
        "kafka.security.protocol",
        "SASL_SSL",
    ).option(
        "kafka.sasl.mechanism",
        "SCRAM-SHA-512",
    ).option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{args.username}" '
        f'password="{args.password}";',
    ).save()

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc(file=sys.stderr)
        raise
