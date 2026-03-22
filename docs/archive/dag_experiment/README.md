# Experiment DAG Archive

This folder contains the experimental DAG version used to evaluate execution strategies for the OSM ingestion pipeline.

Files:

- `core_osm_table_generator_dag_experiment.py`  
  Full experiment DAG with additional instrumentation, run summaries, and strategy-testing logic

- `../dag_experiment_vs_final.diff`  
  Exact diff between the final public DAG and the experiment DAG

Purpose:

- preserve the technical evolution of the project
- document how the execution-strategy experiment was implemented
- provide a readable full script and a precise code comparison artifact

The final active DAG used in the project is located at:

`airflow/dags/core_osm_table_generator_dag.py`