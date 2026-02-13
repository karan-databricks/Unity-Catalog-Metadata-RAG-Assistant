"""
02_metadata_repository_setup.py

Initializes and writes structured metadata into a Delta table.
Simulates Unity Catalog metadata persistence layer.

Author: Karan Singh
Project: Unity Catalog Metadata RAG Assistant
"""

from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)
from pyspark.sql.functions import col
import pandas as pd

# Import metadata builder from Module 01
from notebooks.01_metadata_extraction import build_metadata_dataframe


# ---------------------------------------------------------------------
# Spark Session Initialization
# ---------------------------------------------------------------------

def get_spark_session(app_name: str = "MetadataRepositorySetup") -> SparkSession:
    """
    Initializes Spark session.
    On Databricks, SparkSession is auto-created.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


# ---------------------------------------------------------------------
# Schema Definition
# ---------------------------------------------------------------------

def define_metadata_schema() -> StructType:
    """
    Defines explicit schema for metadata_repository Delta table.
    """

    schema = StructType([
        StructField("object_type", StringType(), True),
        StructField("catalog_name", StringType(), True),
        StructField("schema_name", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("data_type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("sensitivity_flag", StringType(), True),
        StructField("lineage_info", StringType(), True),
        StructField("last_updated", TimestampType(), True),
    ])

    return schema


# ---------------------------------------------------------------------
# Delta Table Write Logic
# ---------------------------------------------------------------------

def write_to_delta(
    spark: SparkSession,
    metadata_df: pd.DataFrame,
    table_name: str = "metadata_repository",
    mode: str = "overwrite"
) -> None:
    """
    Writes metadata DataFrame to Delta table.
    """

    schema = define_metadata_schema()

    spark_df = spark.createDataFrame(metadata_df, schema=schema)

    (
        spark_df
        .write
        .format("delta")
        .mode(mode)
        .saveAsTable(table_name)
    )

    print(f"Delta table '{table_name}' successfully written.")


# ---------------------------------------------------------------------
# Execution Entry Point
# ---------------------------------------------------------------------

if __name__ == "__main__":

    spark = get_spark_session()

    # Step 1: Build metadata DataFrame (from Module 01)
    metadata_df = build_metadata_dataframe()

    # Step 2: Write to Delta table
    write_to_delta(spark, metadata_df)

    print("Metadata repository setup completed.")
