# DAG Description

## Overview

The pipeline is orchestrated using Apache Airflow.

The DAG ensures correct execution order, dependency management, and monitoring of all processing steps.

---

## DAG Structure

The workflow consists of the following tasks:

### 1. Spark Job Submission

- Operator: `SparkKubernetesOperator`
- Launches a Spark job on a Kubernetes cluster
- Reads data from S3, transforms it, and writes results back to S3

---

### 2. Spark Job Monitoring

- Operator: `SparkKubernetesSensor`
- Waits for Spark job completion
- Retrieves execution logs and status

---

### 3. Data Warehouse Layer

- Operator: `SQLExecuteQueryOperator`

Executed steps:
- creation of external table in Greenplum over S3 data
- creation of analytical views:
  - unreliable sellers
  - brand-level report

---

## Key Features

- end-to-end orchestration
- dependency management
- fault tolerance (retry logic)
- integration with distributed processing
