# Unity Catalog Metadata RAG Assistant

Enterprise GenAI Metadata Intelligence Assistant built on Azure Databricks using Unity Catalog, Delta Lake, and Retrieval-Augmented Generation (RAG).

---

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

---

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

---

## Tech Stack

- Azure Databricks
- Unity Catalog
- Delta Lake
- Python
- Vector Search
- Retrieval-Augmented Generation (RAG)
- LLM (Azure OpenAI / Databricks Model Serving)

---

---

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


## Enterprise Relevance

This project demonstrates how Generative AI can be securely integrated into governed data platforms without compromising compliance or observability.

It showcases:

- Metadata-driven RAG architecture
- Production-oriented logging design
- Governance-aware LLM integration
- Scalable embedding pipeline strategy

---

## Future Enhancements

- Role-based metadata visibility enforcement
- Hybrid search (semantic + keyword)
- Response quality evaluation metrics
- Cost monitoring and token tracking dashboard
- CI/CD integration using Databricks Asset Bundles

