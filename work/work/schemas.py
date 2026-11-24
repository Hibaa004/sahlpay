"""Schémas explicites (StructType) partagés par les parties 3 à 6."""
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, TimestampType)

tx_schema = StructType([
    StructField("tx_id", StringType(), False),
    StructField("type", StringType()),
    StructField("payer_wallet", StringType()),
    StructField("payee_id", StringType()),
    StructField("device_id", StringType()),
    StructField("city", StringType()),
    StructField("channel", StringType()),
    StructField("status", StringType()),
    StructField("amount_mad", DoubleType()),
    StructField("event_time", TimestampType()),
])

merchant_schema = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("name", StringType()),
    StructField("city", StringType()),
    StructField("category", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])
