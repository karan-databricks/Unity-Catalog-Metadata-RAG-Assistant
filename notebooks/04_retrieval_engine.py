"""
04_retrieval_engine.py

Implements hybrid retrieval logic:
- Structured filtering
- Vector similarity search
- Top-K ranking

Author: Karan Singh
Project: Unity Catalog Metadata RAG Assistant
"""

from typing import List
from pyspark.sql import SparkSession
import numpy as np


# ---------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------

def get_spark_session(app_name: str = "RetrievalEngine") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# ---------------------------------------------------------------------
# Embedding Utility (Must match embedding pipeline logic)
# ---------------------------------------------------------------------

def mock_generate_embedding(text: str, dim: int = 384) -> List[float]:
    """
    Deterministic embedding generator (must match embedding pipeline).
    """

    np.random.seed(abs(hash(text)) % (10 ** 8))
    return np.random.rand(dim).tolist()


# ---------------------------------------------------------------------
# Cosine Similarity
# ---------------------------------------------------------------------

def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    v1 = np.array(vec1)
    v2 = np.array(vec2)

    return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))


# ---------------------------------------------------------------------
# Hybrid Retrieval
# ---------------------------------------------------------------------

def retrieve_metadata(
    spark: SparkSession,
    query: str,
    top_k: int = 3,
    sensitivity_filter: str = None
):
    """
    Performs hybrid retrieval:
    1. Structured filtering
    2. Vector similarity ranking
    """

    embeddings_df = spark.table("metadata_repository_embeddings")
    rows = embeddings_df.collect()

    # Step 1: Structured Filtering
    if sensitivity_filter:
        rows = [r for r in rows if r.sensitivity_flag == sensitivity_filter]

    # Step 2: Generate query embedding
    query_embedding = mock_generate_embedding(query)

    # Step 3: Compute similarity
    scored_results = []

    for row in rows:
        similarity = cosine_similarity(query_embedding, row.embedding_vector)

        scored_results.append({
            "object_type": row.object_type,
            "table_name": row.table_name,
            "column_name": row.column_name,
            "semantic_summary": row.semantic_summary,
            "score": similarity
        })

    # Step 4: Sort and select top-k
    ranked_results = sorted(
        scored_results,
        key=lambda x: x["score"],
        reverse=True
    )

    return ranked_results[:top_k]


# ---------------------------------------------------------------------
# Execution Entry
# ---------------------------------------------------------------------

if __name__ == "__main__":
    spark = get_spark_session()

    sample_query = "Which tables contain PII?"

    results = retrieve_metadata(
        spark,
        query=sample_query,
        top_k=3,
        sensitivity_filter="PII"
    )

    print("Top Retrieved Results:")
    for r in results:
        print(r)
