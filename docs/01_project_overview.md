# Project Overview — Berlin POI OSM Pipeline

## Objective

This project is part of a larger system developed during my internship, focused on building an **AI-driven place recommendation engine for Berlin**.

The goal of the broader system is to allow users to input a single prompt describing their:

* lifestyle
* family situation
* preferences
* personal needs

Based on this input, the system recommends suitable locations in Berlin and assigns a score indicating how well each area matches the user’s profile.

The system is primarily designed for:

* people relocating to Berlin
* visitors exploring the city

---

## Project Context

The overall solution is being developed by a cross-functional team including:

* Data Engineers (DE)
* Data Analysts (DA)
* Data Scientists (DS)
* Machine Learning Engineers (MLE)
* Software Engineers (SE)
* UI/UX Engineers
* Security Engineers

My role was within the **Data Engineering team**, focusing on building the data ingestion layer that feeds the system with reliable and up-to-date location-based data.

---

## Why OpenStreetMap (OSM)

A key early step in the project was identifying suitable data sources.

OpenStreetMap (OSM) was selected as the primary source because it provides:

* global, open-access geospatial data
* rich Point-of-Interest (POI) coverage
* detailed tagging system (e.g. hotels, supermarkets, amenities)
* frequent community-driven updates
* permissive licensing (ODbL)

For a system modeling real-world living conditions, OSM offers the best balance between:

* data availability
* coverage
* flexibility
* and openness

---

## Exploration Phase

The initial workflow involved **layer-by-layer exploration**.

Each task followed a similar pattern:

1. Select a topic (e.g. hotels in Berlin)
2. Analyze:

   * available OSM tags
   * data completeness
   * variability across records
3. Evaluate:

   * usefulness of the data for the recommendation system
   * potential enrichment sources
4. Design a transformation approach

This phase revealed two key challenges:

* high variability in OSM data structure across layers
* lack of scalability in manual, layer-by-layer processing

---

## From Exploration to Automation

Based on the exploration work, the main engineering challenge became:

> How can we automatically ingest and maintain multiple POI layers without rewriting code for each one?

The solution was to design a **config-driven ingestion pipeline**.

---

## Standardization Strategy

A standardized schema was introduced to ensure consistency across all POI tables.

This included:

* a set of core columns shared by all layers
* layer-specific fields derived from OSM tags
* consistent handling of:

  * geometry (Point, EPSG:4326)
  * administrative areas (district, neighborhood)
  * consistent and scalable schema design (shared core columns combined with layer-specific extensions)

Standardization was based on:

* team-defined requirements
* patterns identified during exploration

---

## Automation with Airflow and Docker

The final system is built around a **single dynamic Airflow DAG**, running inside a Docker-based environment.

Key characteristics:

* processes multiple POI layers (30+ tables)
* reads configuration from JSON files
* automatically:

  * creates tables
  * fetches OSM data
  * applies transformations
  * loads data into PostgreSQL/PostGIS

The pipeline is designed to run on a schedule (e.g. monthly), ensuring that:

> the system always works with fresh, up-to-date data

This is critical because urban environments continuously evolve.

---

## Config-Driven Design

Instead of hardcoding logic per table, the pipeline is driven by two configuration files:

* `core_columns.json` → shared schema across all tables
* `osm_tables.json` → layer-specific definitions (tags + unique columns)

To add a new POI layer, an engineer only needs to:

1. define the layer in `osm_tables.json`
2. rerun the DAG

No code changes are required.

---

## Execution Strategy Experiment

An experiment was conducted to evaluate different execution strategies:

* pure parallel execution
* batched parallel execution

The goal was to balance:

* performance
* system stability
* database load

Final decision:

> **Batched execution** was selected as the preferred strategy

This provides more controlled resource usage with minimal performance trade-off.

---

## Outcome

This project delivers a scalable and production-ready data ingestion pipeline that:

* transforms raw OSM data into structured POI layers
* supports multiple domains (hotels, supermarkets, schools, etc.)
* is easily extensible via configuration
* runs automatically via Airflow
* maintains data freshness over time
* documents engineering decisions and trade-offs

The result is a robust foundation for building location-based intelligence systems,
such as the AI recommendation engine this project supports.

---

## Key Takeaway

> A flexible, config-driven pipeline enables scalable geospatial data ingestion without sacrificing consistency or maintainability.
