cat > LICENSE << 'EOF'
MIT License
Copyright (c) 2025
Permission is hereby granted, free of charge, to any person obtaining a copy...


# Retail Lakehouse (Databricks + Delta)

## Overview

Medallion architecture (Bronze → Silver → Gold) on Delta Lake using a public e-commerce dataset.

## Stack

- Databricks Community Edition
- Delta Lake
- Power BI Desktop or Streamlit (for visuals)

## Repo Structure

- /data                Raw CSVs (optional reference)
- /notebooks           Databricks notebooks (Bronze/Silver/Gold)
- /notebooks/sql       SQL used inside notebooks
- /sql                 Gold views, ad-hoc queries
- /docs                Diagrams and screenshots

## How to Run

1) Upload CSVs to DBFS (`/FileStore/retail/*.csv`)
2) Run `01_bronze_ingest.py`
3) Run `02_silver_conform.py`
4) Run `03_gold_marts.py`
5) Connect BI tool to Gold tables

## Diagrams

See docs/architecture.png

## Dataset

Use a public e-commerce dataset (e.g., Olist or Online Retail II). Place CSVs in `/FileStore/retail/`.

EOF
