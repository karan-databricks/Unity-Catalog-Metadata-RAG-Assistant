"""
06_audit_logging.py

Implements enterprise-grade audit logging for RAG interactions.

Author: Karan Singh
Project: Unity Catalog Metadata RAG Assistant
"""

import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from notebooks.05_rag_inference import run_rag_query
from notebooks.04_retrieval_engine import retrieve_metadata


# ---------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------

def get_spark_session(app_name: str = "RAGAuditLogging") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# ---------------------------------------------------------------------
# Audit Logging Function
# ---------------------------------------------------------------------

def log_rag_interaction(
    spark: SparkSession,
    user_query: str,
    top_k: int = 3,
    sensitivity_filter: str = None
):
    """
    Executes RAG pipeline and logs full interaction.
    """

    query_id = str(uuid.uuid4())
    timestamp = datetime.utcnow()

    try:
        # Step 1: Retrieve metadata
        retrieved_results = retrieve_metadata(
            spark,
            query=user_query,
            top_k=top_k,
            sensitivity_filter=sensitivity_filter
        )

        # Step 2: Execute RAG inference
        response = run_rag_query(
            spark,
            user_query=user_query,
            top_k=top_k,
            sensitivity_filter=sensitivity_filter
        )

        # Step 3: Determine status
        if not retrieved_results:
            status = "NO_CONTEXT"
        else:
            status = "SUCCESS"

        # Step 4: Prepare audit record
        audit_record = [{
            "query_id": query_id,
            "user_query": user_query,
            "retrieved_object_ids": [
                f"{r['table_name']}.{r['column_name']}"
                for r in retrieved_results
            ],
            "retrieved_context": "\n\n".join(
                [r["semantic_summary"] for r in retrieved_results]
            ),
            "llm_response": response,
            "response_status": status,
            "token_usage": 0,  # Placeholder (real LLM will provide)
            "estimated_cost": 0.0,  # Placeholder (real LLM integration)
            "timestamp": timestamp
        }]

        audit_df = spark.createDataFrame(audit_record)

        (
            audit_df
            .write
            .format("delta")
            .mode("append")
            .saveAsTable("metadata_rag_audit_logs")
        )

        print("RAG interaction logged successfully.")

        return response

    except Exception as e:
        print(f"Error during RAG execution: {str(e)}")
        return "Execution error occurred."


# ---------------------------------------------------------------------
# Execution Entry
# ---------------------------------------------------------------------

if __name__ == "__main__":

    spark = get_spark_session()

    query = "Which tables contain PII?"

    result = log_rag_interaction(
        spark,
        user_query=query,
        top_k=3,
        sensitivity_filter="PII"
    )

    print("Final RAG Response:")
    print(result)
