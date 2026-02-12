# Unity Catalog Metadata RAG Assistant

Enterprise GenAI Metadata Intelligence Assistant built on Azure Databricks using Unity Catalog, Delta Lake, and Retrieval-Augmented Generation (RAG).

## Executive Summary

Large-scale data platforms often struggle with metadata discoverability. 
Data engineers and analysts face challenges in understanding:

- Table lineage
- Column definitions
- Data ownership
- Sensitivity (PII) classification
- Data quality and trust levels

This project implements a Retrieval-Augmented Generation (RAG) system that enables conversational querying of Unity Catalog metadata in a governed and auditable manner.

The system integrates enterprise metadata with LLM-powered intelligence while maintaining governance, access control, and observability.

## Architecture Overview

### System Architecture Diagram:
![Unity Catalog Metadata RAG Architecture](architecture/unity_catalog_metadata_rag_architecture.png)

## Key Capabilities

- Conversational querying of table and column metadata
- Lineage-aware contextual responses
- PII-aware retrieval filtering
- Governed metadata access using Unity Catalog
- Audit logging of all LLM interactions
- Enterprise-ready RAG pipeline design

## Tech Stack

- Azure Databricks
- Unity Catalog
- Delta Lake
- Python
- Vector Search
- Retrieval-Augmented Generation (RAG)
- LLM (Azure OpenAI / Databricks Model Serving)

## Metadata Repository Schema

The system maintains a structured Delta table to store extracted Unity Catalog metadata.

**Table: metadata_repository**

| Column Name        | Type      | Description |
|-------------------|----------|-------------|
| object_type       | STRING   | Indicates whether metadata represents a table or column |
| catalog_name      | STRING   | Unity Catalog name |
| schema_name       | STRING   | Schema name |
| table_name        | STRING   | Table name |
| column_name       | STRING   | Column name (nullable for table-level metadata) |
| data_type         | STRING   | Data type of the column |
| description       | STRING   | Table or column description |
| owner             | STRING   | Data owner |
| sensitivity_flag  | STRING   | PII or NON-PII classification |
| lineage_info      | STRING   | Upstream lineage description |
| last_updated      | TIMESTAMP| Metadata update timestamp |

This structured metadata layer enables hybrid filtering (structured + semantic retrieval) within the RAG pipeline.

## Embedding & Chunking Strategy

Unlike traditional document-based RAG systems, this solution embeds structured metadata summaries rather than unstructured documents.

### Table-Level Chunk Structure

Each table-level metadata record is transformed into a structured semantic summary before embedding:

Example:

Table: sales_gold  
Catalog: main  
Schema: finance  
Owner: data_eng_team  
Sensitivity: NON-PII  
Description: Aggregated sales data used for executive reporting.  
Lineage: Derived from sales_silver and customer_silver tables.

### Column-Level Chunk Structure

Each column-level metadata entry is embedded as a contextual metadata summary:

Example:

Column: customer_email  
Table: customer_gold  
Data Type: STRING  
Sensitivity: PII  
Description: Email address of registered customer.  
Lineage: Sourced from CRM system.

This structured semantic formatting improves retrieval precision and enables hybrid filtering (e.g., PII-aware retrieval combined with vector similarity).

## Retrieval Strategy (Hybrid + Governance-Aware)

The system implements a hybrid retrieval mechanism combining structured metadata filtering with semantic vector similarity search.

### Retrieval Flow

1. **Structured Filtering Layer**
   - Filter by catalog, schema, or sensitivity_flag (e.g., PII)
   - Enforce governance constraints before vector search

2. **Vector Similarity Search**
   - Perform top-K semantic similarity search on filtered metadata embeddings
   - Retrieve most contextually relevant table/column entries

3. **Context Assembly**
   - Aggregate retrieved metadata summaries
   - Construct controlled prompt for LLM inference

### Example Query Handling

Query: *"Which tables contain PII?"*

- Apply structured filter: `sensitivity_flag = 'PII'`
- Perform vector similarity on filtered metadata
- Return most relevant metadata entries

This approach ensures governance compliance while maintaining semantic intelligence.


## LLM Guardrails & Anti-Hallucination Strategy

To ensure reliability and governance compliance, the system enforces strict grounding and structured response rules.

### Prompt Design Principles

The LLM is instructed to:

- Act strictly as a Unity Catalog Metadata Assistant.
- Use ONLY the provided metadata context.
- Never assume schema structure, lineage, or ownership.
- Avoid generating hypothetical table or column details.
- If information is missing, respond exactly with:
  "Metadata not available in knowledge base."

### Controlled Prompt Template

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
{retrieved_metadata_chunks}

User Question:
{user_query}

### Anti-Hallucination Mechanisms

- Strict context injection
- Structured filtering before retrieval
- Hybrid (semantic + metadata) search
- Controlled response formatting
- Audit logging of all LLM responses
- Deterministic temperature configuration (low creativity setting)

## Audit Logging & Observability Design

To ensure traceability, governance compliance, and production monitoring, all RAG interactions are logged in a structured Delta table.

**Table: metadata_rag_audit_logs**

| Column Name              | Type      | Description |
|--------------------------|----------|-------------|
| query_id                 | STRING   | Unique identifier for each query |
| user_query               | STRING   | User’s original question |
| retrieved_object_ids     | ARRAY    | List of metadata objects retrieved |
| retrieved_context        | STRING   | Combined context injected into prompt |
| llm_response             | STRING   | Final LLM-generated response |
| response_status          | STRING   | SUCCESS / NO_CONTEXT / ERROR |
| token_usage              | INT      | Total tokens consumed |
| estimated_cost           | DOUBLE   | Approximate LLM cost |
| timestamp                | TIMESTAMP| Query execution time |

### Observability Strategy

The system enables:

- Full traceability of user queries
- Context-to-response mapping
- Hallucination auditing
- Cost monitoring per query
- Governance compliance tracking
- PII-aware interaction logging

This observability layer ensures the RAG assistant can be safely deployed in enterprise data environments.

## Enterprise Relevance

This project demonstrates how Generative AI can be securely integrated into governed data platforms without compromising compliance or observability.

It showcases:

- Metadata-driven RAG architecture
- Production-oriented logging design
- Governance-aware LLM integration
- Scalable embedding pipeline strategy

## Future Enhancements

- Role-based metadata visibility enforcement
- Hybrid search (semantic + keyword)
- Response quality evaluation metrics
- Cost monitoring and token tracking dashboard
- CI/CD integration using Databricks Asset Bundles

