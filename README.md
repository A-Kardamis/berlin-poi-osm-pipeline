# Berlin POI OSM Pipeline

A **config-driven geospatial data pipeline** that ingests, standardizes, and refreshes Berlin Point-of-Interest (POI) data from OpenStreetMap using **Apache Airflow**, **Docker**, and **PostgreSQL/PostGIS**.

This system dynamically generates and processes multiple POI layers through a single DAG, enabling scalable ingestion without hardcoded table logic.

Developed during a **Data Engineering internship**, the project contributes to a broader location-intelligence system aimed at supporting AI-driven recommendations for urban living and exploration.

---

## Project Purpose

This project addresses a real-world data engineering challenge:

* ingest multiple POI categories from OpenStreetMap
* handle heterogeneous tag structures across layers
* enforce a consistent schema across all tables
* allow onboarding of new layers **without code changes**
* support repeatable refresh of continuously evolving geospatial data

The result is a **single dynamic Airflow pipeline** driven entirely by configuration.

---

## Key Features

* **Dynamic Airflow DAG**

  * one DAG processes all POI layers

* **Config-driven architecture**

  * shared schema via `core_columns.json`
  * layer definitions via `osm_tables.json`

* **Scalable table onboarding**

  * new layers added without modifying DAG code

* **Geospatial processing**

  * OSM extraction using tag filters
  * standardized geometry handling

* **Full refresh strategy**

  * tables rebuilt on each run (no incremental complexity)

* **Execution strategies**

  * pure parallel
  * batched execution (experiment-backed decision)

* **Dockerized environment**

  * reproducible local orchestration with Airflow

---

## Repository Structure

```text
berlin-poi-osm-pipeline/
├── README.md
├── .gitignore
├── LICENSE
│
├── airflow/
│   ├── dags/
│   │   └── core_osm_table_generator_dag.py
│   ├── .env.example
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── requirements.txt
│
├── config/
│   ├── core_columns.json
│   ├── lor_ortsteile.geojson
│   └── osm_tables.json
│
├── data_reference/
│   └── wikidata_stars_candidates.csv
│
├── docs/
│   ├── 01_project_overview.md
│   ├── 02_initial_layer_analysis.md
│   ├── 03_data_model_and_column_strategy.md
│   ├── 04_pipeline_architecture.md
│   ├── 05_airflow_and_docker_setup.md
│   ├── 06_execution_strategy_experiment.md
│   ├── 07_final_design_decisions.md
│   ├── images/
│   │   ├── airflow_dag_graph.png
│   │   ├── airflow_mapped_tasks_batch.png
│   │   └── airflow_mapped_tasks_parallel.png
│   └── archive/
│       ├── dag_experiment/
│       │   ├── core_osm_table_generator_dag_experiment.py
│       │   └── README.md
│       ├── experiment_results/
│       │   └── osm_experiment_results.csv
│       └── dag_experiment_vs_final.diff
│
└── notebooks/
    └── osm_layer_exploration_hotels.ipynb
```

---

## Core Technical Idea

> **Separate pipeline logic from layer configuration**

The DAG remains stable, while behavior is defined externally through JSON.

### `core_columns.json`

Defines the shared schema across all POI tables.

### `osm_tables.json`

Defines, per layer:

* table name
* OSM extraction tags
* layer-specific attributes

This allows new layers to be added without modifying pipeline logic.

---

## Pipeline Overview

1. Airflow triggers the DAG
2. configuration files are loaded
3. pipeline iterates through configured layers
4. OSM data is fetched per layer
5. shared transformations are applied
6. layer-specific attributes are appended
7. tables are fully refreshed in PostgreSQL/PostGIS
8. ingestion metadata is recorded

---

## Execution Strategy

Two strategies were evaluated:

* **Pure Parallel**
* **Batch Execution**

While pure parallel offered maximum speed, batch execution provided better stability and resource control in a constrained local environment.

Details and results:

```
docs/06_execution_strategy_experiment.md
```

---

## Documentation Guide

The `docs/` folder contains full project documentation:

* project context and objectives
* layer exploration and analysis
* schema and configuration strategy
* pipeline architecture
* Airflow + Docker setup
* execution experiments
* final engineering decisions

Archived materials:

```
docs/archive/
```

---

## Local Execution

The pipeline runs locally via Docker:

1. configure environment variables
2. start Docker services
3. access Airflow UI
4. trigger the DAG
5. monitor execution

Full setup:

```
docs/05_airflow_and_docker_setup.md
```

---

## Design Principles

* configuration over hardcoding
* consistency with flexibility
* simplicity over unnecessary complexity
* reproducibility via containerization
* controlled execution over maximum concurrency

---

## Outcome

This project delivers a **scalable, production-style ingestion pipeline** for geospatial POI data.

It demonstrates:

* Airflow-based orchestration
* config-driven pipeline design
* geospatial data processing
* execution strategy evaluation
* real-world engineering trade-offs

---

## Author

Developed as part of a Data Engineering internship focused on geospatial data pipelines and location-intelligence systems.

---

## License

MIT License
