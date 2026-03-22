# Final Design Decisions

## Overview

This project required designing a flexible, scalable, and maintainable pipeline for ingesting OpenStreetMap (OSM) data into structured database tables.

Several key design decisions were made to balance performance, simplicity, and extensibility.

---

## JSON-Driven Configuration

The pipeline is fully driven by external JSON configuration files:

- `core_columns.json` → defines shared columns across all tables  
- `osm_tables.json` → defines table-specific configurations  

This design allows:

- adding new tables without modifying DAG code  
- modifying schema structure dynamically  
- separating logic from configuration  

This was a critical decision to ensure long-term scalability.

---

## Single DAG for Multi-Table Processing

Instead of creating one DAG per table, a single dynamic DAG was implemented.

This DAG:

- loops through all configured tables  
- dynamically generates tasks  
- processes each table independently  

This approach reduces code duplication and centralizes orchestration logic.

---

## Execution Strategy Flexibility

Two execution strategies were implemented:

- **Pure Parallel Execution**
- **Batch Execution**

This allows the pipeline to adapt to different resource constraints.

The strategy is controlled through environment variables, enabling runtime configuration without code changes.

---

## Full Refresh Strategy (No Incremental Updates)

The pipeline follows a **full refresh approach**, meaning:

- tables are rebuilt on each run  
- no row-level comparison or incremental logic is used  

This decision was based on:

- relatively small dataset sizes (hundreds to a few thousand rows)  
- lower computational cost compared to maintaining incremental logic  
- simpler and more reliable pipeline behavior  

---

## Minimal Transformations on Layer-Specific Columns

Only shared (core) columns are standardized and transformed.

Layer-specific columns are:

- ingested as-is from OSM  
- lightly cleaned (e.g., column name normalization)  

This avoids over-engineering and keeps the pipeline flexible for diverse data structures.

---

## Column Name Normalization

To ensure compatibility with PostgreSQL:

- special characters in column names are replaced with `_`  
- naming inconsistencies are standardized  

This prevents runtime errors while preserving original data structure as much as possible.

---

## Use of Airflow + Docker

The system uses:

- **Airflow** → orchestration and scheduling  
- **Docker** → environment consistency  

This enables:

- reproducible local development  
- easy transition to production environments  
- controlled execution and monitoring  

---

## Summary

The final design prioritizes:

- flexibility through configuration  
- simplicity in execution logic  
- scalability across many tables  
- maintainability over time  

The result is a robust pipeline capable of supporting ongoing data ingestion needs for a location-based recommendation system.