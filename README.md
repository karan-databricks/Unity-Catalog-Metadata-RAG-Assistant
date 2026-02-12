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

Unity Catalog Metadata  
↓  
Metadata Extraction Pipeline (Spark)  
↓  
Delta Metadata Repository  
↓  
Chunking & Embedding Pipeline  
↓  
Vector Search Index  
↓  
Retriever (Top-K + Metadata Filtering)  
↓  
Prompt Construction  
↓  
LLM Inference  
↓  
Audit Logging (Delta)

---

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

