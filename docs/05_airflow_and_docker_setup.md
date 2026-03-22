# Airflow and Docker Setup

## Objective

This project uses **Docker** and **Apache Airflow** to run the OSM ingestion pipeline in a reproducible local environment.

The setup was designed to:

* keep the runtime environment consistent
* isolate Airflow and database dependencies
* support scheduled pipeline execution
* make configuration changes possible without editing pipeline code

---

## Why Docker Was Used

Docker was used to provide a stable and reproducible execution environment for the pipeline.

This avoids problems such as:

* local dependency conflicts
* inconsistent Python environments
* manual setup differences across machines

In this project, Docker is responsible for running:

* PostgreSQL
* Airflow initialization
* Airflow webserver
* Airflow scheduler

This makes it possible to run the full orchestration environment locally in a controlled way.

---

## Why Airflow Was Used

Apache Airflow was selected because the final system needed:

* scheduled execution
* orchestration across many tables
* configurable runtime behavior
* clear operational visibility

Airflow is a good fit because it provides:

* DAG-based workflow orchestration
* dynamic task mapping
* retries and scheduling
* UI-based run monitoring

The ingestion pipeline is intended to run on a recurring schedule so that location-based data remains fresh over time.  

The ability to dynamically generate tasks based on external configuration (JSON-driven tables) was a key requirement for this project.

---

## Setup Overview

The local setup is composed of the following main files:

* `airflow/docker-compose.yml`
* `airflow/Dockerfile`
* `airflow/requirements.txt`
* `airflow/.env.example`

Together, these files define:

* the containers
* the Python environment
* the Airflow runtime settings
* the pipeline execution controls

---

## Docker Compose Services

The Docker Compose setup defines four services.

### 1. `postgres`

This container provides the PostgreSQL database used by Airflow for metadata storage.

It is configured with:

* `POSTGRES_USER=airflow`
* `POSTGRES_PASSWORD=airflow`
* `POSTGRES_DB=airflow`

A named Docker volume is used so metadata persists between restarts.

---

### 2. `airflow-init`

This is a one-time initialization service.

Its role is to:

* wait for PostgreSQL to become healthy
* run Airflow database migrations
* create the local Airflow admin user

This service is not intended to remain running after initialization.

---

### 3. `airflow-webserver`

This service runs the Airflow web interface.

It allows the user to:

* open the DAG UI
* inspect tasks
* trigger DAG runs manually
* monitor execution results

In this setup, the Airflow UI is exposed on:

```text
http://localhost:8081
```

---

### 4. `airflow-scheduler`

This service runs the Airflow scheduler.

Its role is to:

* detect DAGs
* schedule runs
* execute task orchestration logic

Without the scheduler, the DAG would not be executed.

---

## Executor Choice

The setup uses:

```text
LocalExecutor
```

This means Airflow executes tasks locally inside the scheduler/webserver environment,
without requiring an external queue system such as Redis or Celery.

This keeps the local architecture simpler while still supporting parallel task execution.

It is a good fit for this project because:

* the environment is local
* the pipeline is orchestration-heavy, but still manageable on a single machine
* it avoids additional infrastructure complexity during development

---

## Environment Configuration

Runtime behavior is controlled through environment variables defined in `.env`.

The provided `.env.example` documents the expected variables.

Key variables include:

### Airflow configuration

* `AIRFLOW__WEBSERVER__SECRET_KEY`
* `AIRFLOW_CONN_OSM_POSTGRES`

### Pipeline strategy controls

* `OSM_STRATEGY`
* `OSM_MAX_ACTIVE_TIS_PER_DAGRUN`
* `OSM_BATCH_SIZE`
* `OSM_MAX_ACTIVE_BATCHES`

### Retry behavior

* `OSM_TASK_RETRIES`
* `OSM_RETRY_DELAY_MINUTES`

This design makes it possible to change runtime behavior without modifying the DAG code.

---

## Runtime Strategy Controls

The pipeline supports two execution modes:

* `pure_parallel`
* `batch`

These are controlled through:

```text
OSM_STRATEGY
```

When running in pure parallel mode, concurrency is controlled through:

```text
OSM_MAX_ACTIVE_TIS_PER_DAGRUN
```

When running in batch mode, behavior is controlled through:

```text
OSM_BATCH_SIZE
OSM_MAX_ACTIVE_BATCHES
```

This allows the same DAG to be reused for both high-throughput and more controlled execution patterns.

---

## Docker Image Customization

The Airflow image is extended through a custom `Dockerfile`.

This is necessary because the project depends on geospatial tooling that is not included in the base image by default.

The Dockerfile installs:

* `gdal-bin`
* `libgdal-dev`

It then installs Python dependencies from `requirements.txt`.

This enables the Airflow environment to support:

* pandas
* geopandas
* osmnx
* shapely
* psycopg2

These packages are required for the geospatial extraction and transformation workflow.

---

## Volume Mounts

The Compose setup mounts project files into the Airflow containers:

* `./dags:/opt/airflow/dags`
* `../config:/opt/airflow/config:ro`
* `./logs:/opt/airflow/logs`

This is important because it allows Airflow to access:

* the active DAG code
* the JSON configuration files
* run logs

The config directory is mounted as read-only to avoid accidental modification from inside the container.

---

## How the DAG Is Run

In practice, the DAG is triggered through the **Airflow UI**.

Typical workflow:

1. start the Docker services
2. open the Airflow webserver
3. locate the DAG
4. trigger the DAG run manually
5. monitor task progress in the UI

This made development and testing easier, especially while validating:

* mapped table tasks
* execution strategy changes
* environment variable behavior

---

## Why This Setup Fits the Project

This setup was chosen because it balances:

* reproducibility
* operational visibility
* simplicity
* flexibility

It supports the project requirements without introducing unnecessary infrastructure complexity.

In particular, it enables:

* a local but production-style orchestration workflow
* controlled experimentation with execution strategies
* easy configuration updates through `.env`
* integration with the dynamic multi-table ingestion DAG

---

## Outcome

The Airflow + Docker setup provides a stable local orchestration environment for the Berlin POI ingestion pipeline.

It allows the pipeline to:

* run consistently across environments
* process many POI layers through one DAG
* expose execution through a UI
* support configurable scheduling and execution strategy
* remain easy to maintain during development and testing

This setup forms the operational backbone of the project.

---

## Summary

This setup provides a reproducible and flexible local environment for orchestrating the OSM ingestion pipeline.

By combining Docker and Airflow, the system achieves:

- consistent environment setup across machines  
- configurable execution without code changes  
- support for parallel and batched processing strategies  
- visibility into pipeline execution through the Airflow UI  

This approach allowed efficient experimentation with execution strategies while maintaining a stable and production-like workflow.