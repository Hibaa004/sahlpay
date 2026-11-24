"""Configuration Spark commune à tous les notebooks / scripts SahlPay."""
from pyspark.sql import SparkSession

BASE_PKGS = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "io.delta:delta-spark_2.12:3.2.0",
]
GRAPHFRAMES = "graphframes:graphframes:0.8.3-spark3.5-s_2.12"
LAKE = "s3a://lake"


def get_spark(app_name, graphframes=False):
    pkgs = BASE_PKGS + ([GRAPHFRAMES] if graphframes else [])
    b = (SparkSession.builder.appName(app_name)
         .master("spark://spark-master:7077")
         .config("spark.driver.host", "jupyter")
         .config("spark.driver.bindAddress", "0.0.0.0")
         .config("spark.jars.packages", ",".join(pkgs))
         .config("spark.jars.repositories", "https://repos.spark-packages.org")
         .config("spark.jars.ivy", "/tmp/ivy")
         .config("spark.executor.memory", "1g")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")   # 200 par défaut : trop pour 4 cœurs
         # MinIO (S3A)
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         # Delta Lake
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog",
                 "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
