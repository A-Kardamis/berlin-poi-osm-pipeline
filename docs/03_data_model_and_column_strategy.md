# Data Model and Column Strategy

## Objective

The OSM ingestion pipeline was designed to support many POI layers without requiring
code changes for each new table.

To achieve that, the schema design was split into two configuration layers:

- `core_columns.json` for shared columns
- `osm_tables.json` for layer-specific definitions

This separation makes the pipeline scalable, easier to maintain, and consistent across tables.

---

## Design Principle

The main design goal was to avoid hardcoding table schemas inside the DAG.

Instead of writing separate ingestion logic for each POI layer, the system uses
configuration files to define:

- what every table must contain
- what is common across existing layers
- what is unique to a specific layer
- which OSM tags should be used for extraction

This allows the same DAG to process many tables using shared logic.

---

## Shared Schema: `core_columns.json`

`core_columns.json` defines the columns that form the common structure of all generated tables.

It is split into two groups:

- `must_columns`
- `common_columns`

### Must Columns

These are fields the team decided must exist in every OSM-derived table.

They define the minimum usable schema for downstream analytics and geospatial processing.

Typical examples include:

- `id`
- `name`
- `district`
- `neighborhood`
- `geometry`

### Common Columns

These are fields that were observed across the OSM layers already present in the project
and were considered broadly reusable.

Examples include administrative and contact fields such as:

- `addr_street`
- `addr_postcode`
- `phone_number`
- `email`
- `wheelchair`
- `last_updated`

### Why this matters

This shared schema provides:

- consistency across all POI tables
- predictable downstream usage
- a stable structure for automation
- a controlled place to add or remove common columns in the future

The DAG reads both `must_columns` and `common_columns`, preserves their order,
removes duplicates, and uses the result as the shared final column backbone.

---

## Layer-Specific Schema: `osm_tables.json`

`osm_tables.json` is the layer-definition file that drives the actual multi-table pipeline.

Each table entry defines:

- `table_name`
- `tags`
- `unique_columns`

### `table_name`

This is the target table name to be created and populated by the DAG.

### `tags`

These are the OSM tag filters used during extraction.

They define which features should be fetched from OpenStreetMap for that layer.

Examples include patterns such as:

- `tourism=hotel`
- `amenity=pharmacy`
- `shop=supermarket`

The DAG converts the JSON tag structure into the format expected by OSMnx.

### `unique_columns`

These are the layer-specific fields added on top of the shared schema.

Examples:

- hotels may include fields such as `stars` or `rooms`
- other layers may include fields like `opening_hours`, `brand`, or thematic attributes specific to that POI type

This is the only part that changes when a new layer is introduced.

---

## Why Two JSON Files Were Needed

The project needed a way to:

- schedule ingestion automatically through Airflow
- add or remove layers without changing DAG code
- evolve schemas over time
- keep a shared table structure across many POI layers

A single hardcoded schema inside the DAG would not scale.

The split into:

- shared schema (`core_columns.json`)
- layer-specific configuration (`osm_tables.json`)

solves this by separating:

- what is common
- what varies by layer

This is the central design decision behind the pipeline.

---

## Role of the Exploration Phase

The earlier layer-specific exploration work, especially the hotels prototype,
helped identify:

- which fields are essential in every table
- which attributes are only meaningful for certain layers
- which enrichment and transformation ideas are reusable
- which fields are too inconsistent to generalize

That exploration phase directly informed both JSON files:

- common findings contributed to `core_columns.json`
- layer-specific findings contributed to `osm_tables.json`

---

## Transformation Strategy

The DAG applies transformations mainly to the shared/core part of the schema.

This includes the standardized handling of fields such as:

- identifiers
- geometry
- coordinates
- administrative enrichment
- provenance

For layer-specific unique columns, the strategy is intentionally lighter.

These fields are usually taken from OSM as-is, because building custom transformation
logic for every unique layer attribute would not be practical at scale.

The main exception is column-name normalization.

Because raw OSM keys may contain characters that cause problems in PostgreSQL
(for example `:` or other special characters), the DAG normalizes unique column names
to a safer format using underscores and checks for naming collisions before table creation.

---

## Scalability and Maintenance

This design allows the pipeline to scale from one layer to many layers using the same code.

To add a new layer, the engineer only needs to add a new entry to `osm_tables.json`
following the existing format.

On the next DAG run, the pipeline can:

- detect the new table definition
- create the table
- fetch the matching OSM data
- populate the database using the same shared ingestion flow

This makes the system:

- scalable
- maintainable
- easy to extend
- less error-prone than hardcoded table-by-table logic

---

## Design Trade-Off

The chosen design balances:

- **standardization** through shared core columns
- **flexibility** through per-layer unique columns

Without the shared schema, tables would become inconsistent.
Without per-layer configuration, the pipeline would become rigid and difficult to extend.

Using both allows the project to support many POI layers while still preserving
a coherent data model.

---

## Outcome

The final schema strategy provides:

- a reusable shared table structure
- flexible layer-specific extensions
- configuration-driven onboarding of new tables
- minimal need to modify the DAG when the project grows

This turns the system from a one-off table pipeline into a scalable,
multi-layer geospatial ingestion framework.