import sys
import os
import pyspark
import pyspark.sql
from pyspark import SparkContext

if (len(sys.argv) != 2):
    print("Usage: generate.py OUTPUT")
    exit(1)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

OUTPUT = sys.argv[1]
SRC = f"{SCRIPT_DIR}/../benchmarks/artifacts/tpch_1/lineitem/part-0.parquet"

# Configure spark
conf = pyspark.SparkConf()
conf.setMaster("local[*]")
conf.set("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.iceberg_catalog.type", "hadoop")
conf.set("spark.sql.catalog.iceberg_catalog.warehouse", OUTPUT)
conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
conf.set("spark.driver.memory", "10g")
conf.set("spark.jars", f"{SCRIPT_DIR}/iceberg-spark-runtime-3.4_2.12-1.3.0.jar")
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

# Read source and create iceberg table.
spark.read.parquet(SRC).createOrReplaceTempView("src");
spark.sql(f"CREATE OR REPLACE TABLE iceberg_catalog.iceberg_table TBLPROPERTIES ('format-version'='2', 'write.update.mode'='merge-on-read') AS SELECT * FROM src");

# Partitioned
spark.read.parquet(SRC).createOrReplaceTempView("src");
spark.sql(f"CREATE OR REPLACE TABLE iceberg_catalog.iceberg_table_partition PARTITIONED BY (l_shipmode) TBLPROPERTIES ('format-version'='2', 'write.update.mode'='merge-on-read') AS SELECT * FROM src ORDER BY l_shipmode");


