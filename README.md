# Berlin POI OSM Pipeline

A config-driven geospatial data ingestion pipeline that extracts, standardizes, and refreshes Berlin Point-of-Interest (POI) layers from OpenStreetMap using **Apache Airflow**, **Docker**, and **PostgreSQL/PostGIS**.

This project was developed during a **Data Engineering internship** as part of a larger Berlin location-intelligence system. The broader goal was to support an AI-driven recommendation workflow that helps people identify suitable places to live, visit, or explore in Berlin based on lifestyle and preference signals.

---

## Project Purpose

The project solves a practical data engineering problem:

- multiple POI categories need to be ingested from OpenStreetMap
- each category has different tags and layer-specific attributes
- the schema must stay consistent across all generated tables
- new tables should be added **without modifying DAG code**
- the pipeline should support scheduled refreshes of live urban data

The final result is a **single dynamic Airflow DAG** that processes many POI layers through configuration rather than hardcoded table-by-table logic.

---

## What This Repository Contains

This repository documents the full workflow from early layer exploration to final pipeline automation.

It includes:

- a **reference layer exploration notebook** for hotels
- a **shared data model strategy** using JSON configuration files
- a **dynamic Airflow DAG** that generates and refreshes many POI tables
- a **Docker-based local orchestration environment**
- an **execution strategy experiment** comparing pure parallel vs batched execution
- final design decisions and archived experiment artifacts

---

## Key Features

- **Single dynamic DAG**
  - one Airflow DAG processes all configured POI layers

- **Config-driven architecture**
  - shared schema in `core_columns.json`
  - layer-specific definitions in `osm_tables.json`

- **Scalable onboarding of new tables**
  - new layers are added through configuration, not code changes

- **Geospatial processing**
  - OSM extraction using tag filters
  - standardized geometry handling for downstream database use

- **Full refresh ingestion**
  - tables are rebuilt on each run rather than incrementally merged

- **Execution strategy support**
  - pure parallel mode
  - batched mode
  - final recommendation documented through experiment results

- **Docker + Airflow local environment**
  - reproducible orchestration and testing workflow

---

## Why OpenStreetMap

OpenStreetMap (OSM) was selected as the primary source because it offers:

- open and reusable geospatial data
- broad POI coverage
- rich tag-based structure across many domains
- frequent community-driven updates
- suitability for modeling real-world urban conditions

For this project, OSM provided the best balance of:

- openness
- flexibility
- coverage
- extensibility

---

## Project Phases

### 1. Exploration

The work began with layer-by-layer exploration to understand what was possible with OSM data.

This phase focused on:
- data source evaluation
- tag analysis
- data completeness assessment
- enrichment feasibility
- early transformation logic

The hotels layer was used as the main reference case.

---

### 2. Standardization

The next step was designing a reusable schema strategy.

This led to the split between:

- **shared columns** used across all layers
- **layer-specific columns** defined per POI type

This design made it possible to balance:
- consistency
- flexibility
- scalability

---

### 3. Automation

The final phase was building a single Airflow DAG that could:

- read configuration
- create and refresh tables
- fetch OSM features
- apply shared processing
- load results into PostgreSQL/PostGIS

This transformed a manual workflow into a scalable ingestion system.

---

## Repository Structure

    berlin-poi-osm-pipeline/
├── README.md
├── .gitignore
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
    
   

---

## Core Technical Idea

The central engineering idea of this project is:

> **separate pipeline logic from layer configuration**

The DAG code remains stable, while table behavior is controlled externally through JSON files.

### `core_columns.json`

Defines the shared schema structure used across all generated POI tables.

### `osm_tables.json`

Defines, for each layer:
- table name
- OSM tags used for extraction
- unique layer-specific columns

This means a new POI layer can be onboarded by updating configuration instead of rewriting DAG logic.

---

## How the Pipeline Works

At a high level:

1. Airflow starts the DAG
2. shared and layer-specific JSON configs are loaded
3. the pipeline iterates over configured tables
4. OSM data is fetched for each layer using its tag definition
5. shared/core logic is applied
6. layer-specific fields are appended with minimal transformation
7. the target table is refreshed in PostgreSQL/PostGIS
8. ingestion metadata is logged

The same architecture supports many tables through one orchestration flow.

---

## Execution Strategy

Two execution strategies were evaluated:

- **Pure Parallel**
- **Batch**

The experiment showed that pure parallel could be faster, but batch execution provided more controlled resource usage and safer operational behavior.

The strategy decision and supporting results are documented in:

`docs/06_execution_strategy_experiment.md`

---

## Documentation Guide

The `docs/` folder contains the full project story:

- `01_project_overview.md`  
  overall context and project purpose

- `02_initial_layer_analysis.md`  
  hotels reference-layer exploration

- `03_data_model_and_column_strategy.md`  
  schema strategy and JSON-based design

- `04_pipeline_architecture.md`  
  DAG behavior and table processing flow

- `05_airflow_and_docker_setup.md`  
  local orchestration environment

- `06_execution_strategy_experiment.md`  
  execution strategy comparison and results

- `07_final_design_decisions.md`  
  final engineering trade-offs and rationale

Archived technical comparison artifacts are stored under:

`docs/archive/`

---

## Example Visuals

The repository includes Airflow screenshots showing:

- DAG graph structure
- pure parallel mapped tasks
- batched mapped tasks

These are stored in:

`docs/images/`

---

## Local Execution

The pipeline is intended to run locally through Docker and Airflow.

In general:

1. configure environment variables
2. start the Docker services
3. open the Airflow UI
4. trigger the DAG
5. monitor execution in the Airflow interface

Detailed setup notes are documented in:

`docs/05_airflow_and_docker_setup.md`

---

## Design Principles

The final implementation prioritizes:

- maintainability over hardcoded table logic
- shared standards with layer-specific flexibility
- simple full refreshes over complex incremental logic
- controlled execution over maximum theoretical speed
- reproducibility through Dockerized orchestration

---

## Outcome

This project delivers a production-style ingestion foundation for Berlin POI data.

It demonstrates:

- geospatial data ingestion from OSM
- schema design for multi-layer systems
- dynamic orchestration with Airflow
- Docker-based reproducibility
- execution-strategy evaluation
- practical engineering trade-off decisions

It also serves as a strong example of how exploratory data work can be transformed into a scalable, automated data engineering pipeline.

---

## Author

Developed as part of a Data Engineering internship project focused on Berlin POI ingestion and location-intelligence infrastructure.

## License

This project is licensed under the MIT License.