# Data Maestro Labs (DML) Data Platform

## Table of Contents

- [Overview](#overview)
- [Objectives](#objectives)
- [System Architecture](#system-architecture)
- [Tech Stack](#tech-stack)
- [Platform Folder Structure](#platform-folder-structure)

---

## Overview

This repo contains the structure for the DML data platform.
The aim of this project is to build a data platform for building
end-to-end data pipelines for various analytical or machine learning
projects using only open source tools.

---

## Objectives

- Build using only open source tools.
- Platform will be based on the Extract, Load, Transform design pattern.
- Implement proper CI/CD processes to ensure that the platform is fully
  automated, versioned, and reproducible.

---

## System Architecture


---

## Tech Stack

| Layer           | Tool                               | Role                              |
|-----------------|------------------------------------|:----------------------------------|
| Orchestration   | Dagster                            | Pipeline scheduling & lineage     |
| Data Cataloging | OpenMetadata                       | Metadata, lineage, and discovery  |
| Storage         | duckdb + MinIO + Parquet + Iceberg | Data lakehouse foundation         |
| Ingestion       | Airbyte                            | Source replication                |
| Transformation  | dbt + duckdb + pandas              | Transformations                   |
| Observability   | Elementary                         | Data quality                      |
| Versioning      | DVC                                | Data lineage & reproducibility    |
| Deployment      | Portainer (Docker)                 | Container orchestration on server |


---

## Platform Folder Structure

```bash
data-platform/
├── dagster_project/
│   ├── repository.py # Dagster repository registering all projects
│   ├── shared_ops/   # Shared Dagster ops (e.g., load Parquet, run dbt)
│   ├── projects/
│   │   ├── sample-project/
│   │   │   ├── assets.py # Dagster assets for sales
│   │   │   ├── jobs.py  # Dagster job definitions
│   │   │   └── config/  # Project-specific configs (Airbyte IDs, DuckDB paths)
│   │   └── sample-project-2/
│   │       └── ...
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── sample-project/
│   │   │   ├── staging/
│   │   │   ├── marts/
│   │   │   └── schema.yml
│   │   └── sample-project-2/
│   │       └── ...
│   ├── seeds/
│   └── snapshots/
├── notebooks/                 # ad-hoc exploration
│   └── sales/
├── data/                      # local DuckDB or raw files (also DVC-tracked)
├── dvc.yaml                   # top-level DVC pipelines (if needed)
├── docker/
├── tests/
│   ├── dagster/
│   └── dbt/
└── .github/
```

---
