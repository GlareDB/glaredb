import argparse
import os
import glob
import pyspark
import pyspark.sql


def main():
    parser = argparse.ArgumentParser(
        prog='generate_iceberg',
        description='Generate iceberg test data',
        add_help=True,
    )

    parser.add_argument(
        '--format-version',
        help='Format version for the iceberg spec',
        type=int,
        choices=[1, 2],
        default=2,
    )

    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.realpath(__file__))
    output_dir = f"./iceberg/tables-v{args.format_version}"
    lineitem_src = f"{script_dir}/iceberg/source_data/lineitem.parquet"

    # Find iceberg jars
    jars = glob.glob("./iceberg-spark-runtime-*.jar")
    if len(jars) != 1:
        print("Expected one spark jar in current directory")
        exit(1)
    spark_jar = jars[0]

    # Configure spark
    conf = pyspark.SparkConf()
    conf.setMaster("local[*]")
    conf.set(
        "spark.sql.catalog.iceberg_catalog",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    conf.set("spark.sql.catalog.iceberg_catalog.type", "hadoop")
    conf.set("spark.sql.catalog.iceberg_catalog.warehouse", output_dir)
    conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    conf.set("spark.driver.memory", "10g")
    conf.set("spark.jars", f"{script_dir}/{spark_jar}")
    conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()

    spark.read.parquet(lineitem_src).createOrReplaceTempView("lineitem")

    # TODO:
    # - Ordered tables

    # Simple iceberg table.
    spark.sql(f"""
    CREATE OR REPLACE TABLE iceberg_catalog.lineitem_simple
    TBLPROPERTIES (
        'format-version'='{args.format_version}',
        'write.update.mode'='merge-on-read'
    ) AS SELECT * FROM lineitem
    """)

    # Partitioned by ship mode.
    spark.sql(f"""
    CREATE OR REPLACE TABLE iceberg_catalog.lineitem_partitioned
    PARTITIONED BY (l_shipmode)
    TBLPROPERTIES (
        'format-version'='{args.format_version}',
        'write.update.mode'='merge-on-read'
    ) AS SELECT * FROM lineitem ORDER BY l_shipmode
    """)

    # Versioned table.
    spark.sql(f"""
    CREATE OR REPLACE TABLE iceberg_catalog.lineitem_versioned
    TBLPROPERTIES (
        'format-version'='{args.format_version}',
        'write.update.mode'='merge-on-read'
    ) AS SELECT * FROM lineitem
    """)

    spark.sql("""
    INSERT INTO iceberg_catalog.lineitem_versioned SELECT * FROM lineitem
    """)


if __name__ == '__main__':
    main()
