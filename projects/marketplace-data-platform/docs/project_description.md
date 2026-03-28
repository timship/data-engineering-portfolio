# Project Description

## Overview

This project simulates a real-world Data Engineering workflow for a marketplace platform.

The system processes large volumes of product data and builds analytical datasets used for business analysis, reporting, and decision-making.

---

## Business Goal

The goal of the project is to build a data pipeline and create analytical data marts that provide aggregated metrics for:

- product performance
- category-level insights
- seller reliability

These datasets are intended for analytical teams and downstream consumers.

---

## Data Description

The source dataset is stored in S3 (Data Lake) in parquet format.

Each record represents a product item and includes attributes such as:

- SKU and product information (title, category, brand)
- seller and location data
- stock availability and order counts
- pricing and rating metrics
- product lifecycle (days on sale)

The dataset is enriched with additional calculated metrics during processing.

---

## Objectives

- Build an end-to-end data pipeline
- Process and enrich raw data using Spark
- Store processed data in a Data Lake layer
- Expose data to DWH (Greenplum) via external tables
- Create analytical views for reporting

---

## Learning Focus

The project demonstrates how different components of a data platform interact:

- Data Lake (S3)
- Distributed processing (Spark)
- Orchestration (Airflow)
- Data Warehouse (Greenplum)

It reflects a typical Data Engineering workflow in production-like environments.
