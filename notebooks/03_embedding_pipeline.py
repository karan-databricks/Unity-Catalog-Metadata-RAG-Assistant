"""
03_embedding_pipeline.py

Transforms structured metadata into semantic chunks
and generates embeddings for RAG retrieval.

Author: Karan Singh
Project: Unity Catalog Metadata RAG Assistant
"""

from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.types import ArrayType, FloatType
import numpy as np


# ---------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------

def get_spark_session(app_name: str = "EmbeddingPipeline") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# ---------------------------------------------------------------------
# Semantic Chunk Builder
# ---------------------------------------------------------------------

def build_semantic_summary(row) -> str:
    """
    Converts structured metadata row into semantic text summary.
    """

    base_summary = f"""
    Object Type: {row.object_type}
    Catalog: {row.catalog_name}
    Schema: {row.schema_name}
    Table: {row.table_name}
    Column: {row.column_name if row.column_name else 'N/A'}
    Data Type: {row.data_type if row.data_type else 'N/A'}
    Owner: {row.owner}
    Sensitivity: {row.sensitivity_flag}
    Description: {row.description}
    Lineage: {row.lineage_info}
    """

    return base_summary.strip()


# ---------------------------------------------------------------------
# Mock Embedding Generator (Local Safe Mode)
# ---------------------------------------------------------------------

def mock_generate_embedding(text: str, dim: int = 384) -> List[float]:
    """
    Generates deterministic mock embedding vector.
    Replace with Azure OpenAI or Databricks embedding endpoint in production.
    """

    np.random.seed(abs(hash(text)) % (10 ** 8))
    return np.random.rand(dim).tolist()


# ---------------------------------------------------------------------
# Embedding Pipeline
# ---------------------------------------------------------------------

def generate_embeddings(spark: SparkSession):
    """
    Reads metadata_repository table,
    builds semantic chunks,
    generates embeddings,
    writes enriched table.
    """

    metadata_df = spark.table("metadata_repository")

    # Collect rows to driver for embedding (controlled demo approach)
    rows = metadata_df.collect()

    enriched_records = []

    for row in rows:
        summary = build_semantic_summary(row)
        embedding = mock_generate_embedding(summary)

        enriched_records.append({
            "object_type": row.object_type,
            "catalog_name": row.catalog_name,
            "schema_name": row.schema_name,
            "table_name": row.table_name,
            "column_name": row.column_name,
            "semantic_summary": summary,
            "embedding_vector": embedding,
        })

    enriched_df = spark.createDataFrame(enriched_records)

    (
        enriched_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("metadata_repository_embeddings")
    )

    print("Embeddings generated and stored successfully.")


# ---------------------------------------------------------------------
# Execution Entry
# ---------------------------------------------------------------------

if __name__ == "__main__":
    spark = get_spark_session()
    generate_embeddings(spark)
