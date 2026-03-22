# Initial Layer Analysis — Berlin Hotels

## Objective

The goal of this phase was to identify, evaluate, and prepare data sources
for building a reliable hotel Point-of-Interest (POI) layer for Berlin.

This included:
- data source discovery
- schema design considerations
- enrichment feasibility
- transformation planning

---

## Primary Data Source — OpenStreetMap

OpenStreetMap (OSM) was selected as the **primary data source** due to:

- global coverage
- open licensing (ODbL)
- availability of geospatial POI data
- rich tagging system

### Relevant tags

- tourism=hotel
- tourism=hostel
- tourism=guest_house
- tourism=aparthotel

### Extracted attributes

- name
- accommodation type
- geometry (lat/lon)
- address components (when available)
- contact information (when available)

### Limitations

- inconsistent tag completeness
- varying data quality across locations
- sparse attributes (e.g. rooms, amenities)

---

## Secondary Data Sources (Evaluation)

### Berlin Open Data Portal

- Focus: aggregated tourism statistics
- Limitation: no POI-level hotel dataset

👉 Not suitable as a primary source.

---

### VisitBerlin

- Official tourism platform
- Limitation: no open dataset or API

👉 Excluded due to licensing restrictions.

---

### Wikidata

- Structured knowledge graph
- License: CC0

👉 Used as **supplementary enrichment only**

---

### Commercial Platforms (Booking, Expedia, etc.)

👉 Excluded due to:
- proprietary data
- restrictive licensing

---

## Data Enrichment Strategy

### Guiding Principle

> OpenStreetMap is the authoritative base layer.  
> External sources are **supplementary only** and never overwrite OSM data.

---

## Field-Level Decisions

### Amenities & Accessibility

- Derived exclusively from OSM tags
- No external enrichment applied

**Reason:** lack of reliable open datasets

---

### Contact Information

Fields:
- phone
- website
- email

Approach:
- consolidated multiple OSM tag variants
- normalized into single fields

---

### Room Count

- Source: OSM only
- Coverage: sparse

---

### Star Rating (Wikidata)

Wikidata was used to enrich missing star ratings.

Rules:
- only applied when OSM value is missing
- never overwrites existing values

---

### Address Enrichment (Reverse Geocoding)

- Tool: Nominatim
- Used to fill missing:
  - street
  - house number
  - postal code

---

### Administrative Areas

- Source: Berlin LOR boundaries (GeoJSON)
- Method: spatial join

Enriched fields:
- district
- neighborhood
- district_id
- neighborhood_id

---
### Data Provenance Tracking

To ensure traceability and transparency, each record maintains provenance metadata:

- `data_source` → indicates contributing sources (e.g. `OSM`, `OSM;wikidata`, `OSM;nominatim`)
- `source_ids` → tracks identifiers from each source (e.g. `osm:<id>;wikidata:<QID>`)

Rules:
- enrichment steps append to provenance fields
- no source ever overwrites another
- all transformations remain traceable

This allows:
- auditability of enriched values
- debugging of data inconsistencies
- clear understanding of data lineage
---

### Deduplication

Criteria:
- same normalized name
- within 10 meters distance

Applied after all enrichment steps.

---

## Final Schema Design (Conceptual)

Each record represents a single hotel POI with:

- geospatial data (lat/lon, geometry)
- administrative context (district, neighborhood)
- descriptive attributes (name, type)
- operational attributes (contact info, amenities)
- enrichment fields (star rating, address)
- provenance tracking

---

## Key Challenges

- inconsistent OSM tagging
- sparse attributes (rooms, amenities)
- limited enrichment sources
- balancing completeness vs reliability

---

## Outcome

A structured and enriched hotel POI dataset that:

- uses OSM as a reliable base layer
- incorporates selective enrichment
- maintains data provenance
- aligns with a standardized POI schema