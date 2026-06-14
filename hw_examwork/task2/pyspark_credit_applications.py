from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input CSV path in Object Storage, for example s3a://bucket/path/file.csv")
    parser.add_argument("--output", required=True, help="Output prefix in Object Storage, for example s3a://bucket/path/output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_prefix = args.output.rstrip("/")

    spark = (
        SparkSession.builder.appName("credit-applications-etl")
        .enableHiveSupport()
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("application_id", StringType(), False),
            StructField("event_time", TimestampType(), True),
            StructField("customer_id", StringType(), True),
            StructField("region_code", StringType(), True),
            StructField("product_type", StringType(), True),
            StructField("requested_amount", IntegerType(), True),
            StructField("term_months", IntegerType(), True),
            StructField("credit_score", IntegerType(), True),
            StructField("risk_level", StringType(), True),
            StructField("decision_status", StringType(), True),
            StructField("approved_amount", IntegerType(), True),
            StructField("channel", StringType(), True),
            StructField("employee_review_flag", BooleanType(), True),
            StructField("processing_time_sec", IntegerType(), True),
        ]
    )

    raw = (
        spark.read.option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(schema)
        .csv(args.input)
    )

    applications = (
        raw.withColumn("event_date", F.to_date("event_time"))
        .withColumn(
            "approval_gap",
            F.col("requested_amount") - F.col("approved_amount"),
        )
        .withColumn(
            "is_approved",
            F.when(F.col("decision_status") == "approved", F.lit(1)).otherwise(F.lit(0)),
        )
    )

    daily_stats = (
        applications.groupBy("event_date")
        .agg(
            F.count("*").alias("applications_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("requested_amount").alias("requested_amount_total"),
            F.sum("approved_amount").alias("approved_amount_total"),
            F.avg("processing_time_sec").alias("avg_processing_time_sec"),
            F.avg("is_approved").alias("approval_rate"),
        )
        .orderBy("event_date")
    )

    risk_product_stats = (
        applications.groupBy("risk_level", "product_type")
        .agg(
            F.count("*").alias("applications_count"),
            F.sum("requested_amount").alias("requested_amount_total"),
            F.sum("approved_amount").alias("approved_amount_total"),
            F.avg("credit_score").alias("avg_credit_score"),
            F.avg("is_approved").alias("approval_rate"),
        )
        .orderBy("risk_level", "product_type")
    )

    channel_stats = (
        applications.groupBy("channel")
        .agg(
            F.count("*").alias("applications_count"),
            F.avg("processing_time_sec").alias("avg_processing_time_sec"),
            F.sum(F.col("employee_review_flag").cast("int")).alias("manual_review_flags"),
            F.avg("is_approved").alias("approval_rate"),
        )
        .orderBy("channel")
    )

    applications.write.mode("overwrite").format("parquet").partitionBy("event_date").option(
        "path",
        f"{output_prefix}/applications_clean",
    ).saveAsTable("applications_clean")

    daily_stats.coalesce(1).write.mode("overwrite").format("parquet").option(
        "path",
        f"{output_prefix}/daily_stats",
    ).saveAsTable("daily_stats")

    risk_product_stats.coalesce(1).write.mode("overwrite").format("parquet").option(
        "path",
        f"{output_prefix}/risk_product_stats",
    ).saveAsTable("risk_product_stats")

    channel_stats.coalesce(1).write.mode("overwrite").format("parquet").option(
        "path",
        f"{output_prefix}/channel_stats",
    ).saveAsTable("channel_stats")

    spark.sql("SHOW TABLES").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
