"""
01_metadata_extraction.py

Simulates Unity Catalog metadata extraction and prepares structured
metadata records for downstream RAG processing.

Author: Karan Singh
Project: Unity Catalog Metadata RAG Assistant
"""

from datetime import datetime
from typing import List, Dict
import pandas as pd


# ---------------------------------------------------------------------
# Metadata Simulation Layer
# ---------------------------------------------------------------------

def generate_table_metadata() -> List[Dict]:
    """
    Simulates table-level Unity Catalog metadata.
    """

    current_time = datetime.utcnow()

    table_metadata = [
        {
            "object_type": "table",
            "catalog_name": "main",
            "schema_name": "finance",
            "table_name": "sales_gold",
            "column_name": None,
            "data_type": None,
            "description": "Aggregated sales data used for executive reporting.",
            "owner": "data_eng_team",
            "sensitivity_flag": "NON-PII",
            "lineage_info": "Derived from sales_silver and customer_silver tables.",
            "last_updated": current_time,
        },
        {
            "object_type": "table",
            "catalog_name": "main",
            "schema_name": "crm",
            "table_name": "customer_gold",
            "column_name": None,
            "data_type": None,
            "description": "Master customer dataset containing profile information.",
            "owner": "crm_data_team",
            "sensitivity_flag": "PII",
            "lineage_info": "Sourced from CRM operational database.",
            "last_updated": current_time,
        },
        {
            "object_type": "table",
            "catalog_name": "main",
            "schema_name": "finance",
            "table_name": "revenue_gold",
            "column_name": None,
            "data_type": None,
            "description": "Revenue summary dataset used for financial analytics.",
            "owner": "finance_analytics_team",
            "sensitivity_flag": "NON-PII",
            "lineage_info": "Aggregated from sales_gold and pricing_reference tables.",
            "last_updated": current_time,
        }
    ]

    return table_metadata


def generate_column_metadata() -> List[Dict]:
    """
    Simulates column-level Unity Catalog metadata.
    """

    current_time = datetime.utcnow()

    column_metadata = [
        {
            "object_type": "column",
            "catalog_name": "main",
            "schema_name": "crm",
            "table_name": "customer_gold",
            "column_name": "customer_email",
            "data_type": "STRING",
            "description": "Email address of the registered customer.",
            "owner": "crm_data_team",
            "sensitivity_flag": "PII",
            "lineage_info": "Directly sourced from CRM system.",
            "last_updated": current_time,
        },
        {
            "object_type": "column",
            "catalog_name": "main",
            "schema_name": "crm",
            "table_name": "customer_gold",
            "column_name": "customer_id",
            "data_type": "STRING",
            "description": "Unique identifier assigned to each customer.",
            "owner": "crm_data_team",
            "sensitivity_flag": "NON-PII",
            "lineage_info": "Generated within CRM platform.",
            "last_updated": current_time,
        },
        {
            "object_type": "column",
            "catalog_name": "main",
            "schema_name": "finance",
            "table_name": "sales_gold",
            "column_name": "total_sales_amount",
            "data_type": "DOUBLE",
            "description": "Total sales amount aggregated per transaction.",
            "owner": "data_eng_team",
            "sensitivity_flag": "NON-PII",
            "lineage_info": "Calculated from sales_silver transactional data.",
            "last_updated": current_time,
        }
    ]

    return column_metadata


# ---------------------------------------------------------------------
# DataFrame Assembly
# ---------------------------------------------------------------------

def build_metadata_dataframe() -> pd.DataFrame:
    """
    Combines table-level and column-level metadata into a single DataFrame.
    """

    table_records = generate_table_metadata()
    column_records = generate_column_metadata()

    combined_records = table_records + column_records

    df = pd.DataFrame(combined_records)

    return df


# ---------------------------------------------------------------------
# Execution Entry Point
# ---------------------------------------------------------------------

if __name__ == "__main__":
    metadata_df = build_metadata_dataframe()

    print("Metadata Extraction Completed")
    print("Total Records:", len(metadata_df))
    print(metadata_df.head())

