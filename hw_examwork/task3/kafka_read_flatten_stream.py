from __future__ import annotations

import argparse
import sys
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, from_json, to_timestamp
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--username", required=True, help="Kafka username")
    parser.add_argument("--password", required=True, help="Kafka password")
    parser.add_argument("--output", required=True, help="Output path in Object Storage")
    parser.add_argument("--checkpoint", required=True, help="Checkpoint path in Object Storage")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(
        "Kafka reader args: "
        f"bootstrap_servers={args.bootstrap_servers}, topic={args.topic}, "
        f"username={args.username}, output={args.output}, checkpoint={args.checkpoint}",
        flush=True,
    )
    spark = SparkSession.builder.appName("loan-events-kafka-flatten-stream").getOrCreate()

    schema = StructType(
        [
            StructField("application_id", StringType(), True),
            StructField(
                "customer",
                StructType(
                    [
                        StructField("customer_id", StringType(), True),
                        StructField("region", StringType(), True),
                        StructField("segment", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "loan",
                StructType(
                    [
                        StructField("amount", IntegerType(), True),
                        StructField("term_months", IntegerType(), True),
                        StructField("product_type", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "scoring",
                StructType(
                    [
                        StructField("score", IntegerType(), True),
                        StructField("risk_level", StringType(), True),
                        StructField("model_version", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "documents",
                ArrayType(
                    StructType(
                        [
                            StructField("type", StringType(), True),
                            StructField("status", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField("decision_status", StringType(), True),
            StructField("submitted_at", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("processing_time_sec", IntegerType(), True),
        ]
    )

    kafka_options = {
        "kafka.bootstrap.servers": args.bootstrap_servers,
        "subscribe": args.topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
        "kafka.sasl.jaas.config": (
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{args.username}" '
            f'password="{args.password}";'
        ),
        "startingOffsets": "earliest",
    }

    source = spark.readStream.format("kafka").options(**kafka_options).load()
    parsed = source.select(from_json(col("value").cast("string"), schema).alias("event"))
    flat = (
        parsed.select(
            col("event.application_id").alias("application_id"),
            col("event.customer.customer_id").alias("customer_id"),
            col("event.customer.region").alias("region"),
            col("event.customer.segment").alias("customer_segment"),
            col("event.loan.amount").alias("amount"),
            col("event.loan.term_months").alias("term_months"),
            col("event.loan.product_type").alias("product_type"),
            col("event.scoring.score").alias("score"),
            col("event.scoring.risk_level").alias("risk_level"),
            col("event.scoring.model_version").alias("model_version"),
            explode_outer(col("event.documents")).alias("document"),
            col("event.decision_status").alias("decision_status"),
            to_timestamp(col("event.submitted_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("submitted_at"),
            col("event.channel").alias("channel"),
            col("event.processing_time_sec").alias("processing_time_sec"),
        )
        .select(
            "application_id",
            "customer_id",
            "region",
            "customer_segment",
            "amount",
            "term_months",
            "product_type",
            "score",
            "risk_level",
            "model_version",
            col("document.type").alias("document_type"),
            col("document.status").alias("document_status"),
            "decision_status",
            "submitted_at",
            "channel",
            "processing_time_sec",
        )
    )

    query = (
        flat.writeStream.trigger(once=True)
        .format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc(file=sys.stderr)
        raise
