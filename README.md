# Real Estate Analytics Pipeline using PySpark

## Overview
This project builds an end-to-end Real Estate Data Engineering pipeline using PySpark.
It follows a Bronze → Silver → Gold architecture to process raw scraped property data.

## Architecture
- Bronze Layer: Raw ingestion from CSV to parquet
- Silver Layer: Cleaning, standardization, deduplication, datatype conversions
- Gold Layer: KPI tables for analytics and dashboards

## KPIs Generated
- City-level KPIs (avg rent, avg price, total properties)
- Company-level portfolio KPIs
- Property-type distribution KPIs
- Rental yield analysis table

## Tech Stack
- PySpark
- Parquet Storage
- Data Lake Architecture (Bronze/Silver/Gold)

## Run Pipeline
```bash
python -m src.main
