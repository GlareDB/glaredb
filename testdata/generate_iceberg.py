import sys
import os
import pyspark
import pyspark.sql
from pyspark import SparkContext

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

OUTPUT_DIR = f"{SCRIPT_DIR}/iceberg/tables"
LINEITEM_SRC = f"{SCRIPT_DIR}/iceberg/source_data/lineitem.parquet"

# Configure spark
conf = pyspark.SparkConf()
conf.setMaster("local[*]")
conf.set("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.iceberg_catalog.type", "hadoop")
conf.set("spark.sql.catalog.iceberg_catalog.warehouse", OUTPUT_DIR)
conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
conf.set("spark.driver.memory", "10g")
conf.set("spark.jars", f"{SCRIPT_DIR}/iceberg-spark-runtime-3.4_2.12-1.3.0.jar")
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

spark.read.parquet(LINEITEM_SRC).createOrReplaceTempView("lineitem");

# TODO:
# - v1 format
# - Ordered tables

# Simple iceberg table.
spark.sql(f"""
CREATE OR REPLACE TABLE iceberg_catalog.lineitem_simple
TBLPROPERTIES ('format-version'='2', 'write.update.mode'='merge-on-read')
AS SELECT * FROM lineitem
""");

# Partitioned by ship mode.
spark.sql(f"""
CREATE OR REPLACE TABLE iceberg_catalog.lineitem_partitioned
PARTITIONED BY (l_shipmode) TBLPROPERTIES ('format-version'='2', 'write.update.mode'='merge-on-read')
AS SELECT * FROM lineitem ORDER BY l_shipmode
""");

# Versioned table.
spark.sql(f"""
CREATE OR REPLACE TABLE iceberg_catalog.lineitem_versioned
TBLPROPERTIES ('format-version'='2', 'write.update.mode'='merge-on-read')
AS SELECT * FROM lineitem
""");
spark.sql(f"""
INSERT INTO iceberg_catalog.lineitem_versioned SELECT * FROM lineitem
""");

