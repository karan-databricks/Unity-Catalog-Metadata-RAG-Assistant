"""
05_rag_inference.py

Implements RAG inference layer:
- Retrieves metadata context
- Applies guardrail prompt template
- Generates LLM response (mock mode)
- Returns structured output

Author: Karan Singh
Project: Unity Catalog Metadata RAG Assistant
"""

from pyspark.sql import SparkSession
from typing import List, Dict
from notebooks.04_retrieval_engine import retrieve_metadata


# ---------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------

def get_spark_session(app_name: str = "RAGInference") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# ---------------------------------------------------------------------
# Context Assembly
# ---------------------------------------------------------------------

def assemble_context(retrieved_results: List[Dict]) -> str:
    """
    Combines semantic summaries into LLM context block.
    """

    context_blocks = [r["semantic_summary"] for r in retrieved_results]
    return "\n\n".join(context_blocks)


# ---------------------------------------------------------------------
# Guardrail Prompt Builder
# ---------------------------------------------------------------------

def build_prompt(context: str, user_query: str) -> str:
    """
    Constructs controlled prompt with anti-hallucination guardrails.
    """

    prompt = f"""
You are a Data Platform Metadata Assistant.

Use ONLY the provided metadata context to answer the question.
If the answer is not found in the context, respond exactly:
"Metadata not available in knowledge base."

Provide the answer in the following structured format:

- Object Type:
- Table Name:
- Column Name (if applicable):
- Description:
- Sensitivity:
- Lineage:

Context:
{context}

User Question:
{user_query}
"""

    return prompt.strip()


# ---------------------------------------------------------------------
# Mock LLM (Safe Mode)
# ---------------------------------------------------------------------

def mock_llm_response(context: str, user_query: str) -> str:
    """
    Simulates LLM output using deterministic rule-based extraction.
    Replace with Azure OpenAI call in production.
    """

    if not context:
        return "Metadata not available in knowledge base."

    # Simple rule-based response generator
    return f"""
- Object Type: Retrieved Metadata
- Table Name: Refer to context
- Column Name (if applicable): Refer to context
- Description: Based strictly on retrieved metadata
- Sensitivity: Based on metadata
- Lineage: Based on metadata
""".strip()


# ---------------------------------------------------------------------
# RAG Inference Execution
# ---------------------------------------------------------------------

def run_rag_query(
    spark: SparkSession,
    user_query: str,
    top_k: int = 3,
    sensitivity_filter: str = None
) -> str:
    """
    Executes full RAG pipeline:
    Retrieval → Context → Prompt → LLM → Response
    """

    retrieved_results = retrieve_metadata(
        spark,
        query=user_query,
        top_k=top_k,
        sensitivity_filter=sensitivity_filter
    )

    context = assemble_context(retrieved_results)
    prompt = build_prompt(context, user_query)
    response = mock_llm_response(context, user_query)

    return response


# ---------------------------------------------------------------------
# Execution Entry
# ---------------------------------------------------------------------

if __name__ == "__main__":
    spark = get_spark_session()

    query = "Which tables contain PII?"

    result = run_rag_query(
        spark,
        user_query=query,
        top_k=3,
        sensitivity_filter="PII"
    )

    print("Final RAG Response:")
    print(result)
