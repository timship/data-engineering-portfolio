# Architecture

## Logical Architecture

The system is divided into two main layers:

### 1. Data Lake (S3)

The Data Lake stores raw and processed data in its original format.

Characteristics:
- supports structured and semi-structured data
- schema-on-read approach
- used for scalable storage and preprocessing

### 2. Data Warehouse (Greenplum)

The Data Warehouse is used for analytical queries and reporting.

Characteristics:
- structured data model
- optimized for aggregations and analytics
- supports external tables over S3 data

---

## Key Difference

The main difference between Data Lake and Data Warehouse:

- Data Lake → stores raw, unprocessed data
- Data Warehouse → stores structured, processed data optimized for analysis

---

## Technical Architecture

The pipeline is built using the following components:

### Storage
- **S3** — Data Lake storage for raw and processed data

### Processing
- **Apache Spark (PySpark)** — distributed data processing

### Orchestration
- **Apache Airflow** — workflow scheduling and task orchestration

### Data Warehouse
- **Greenplum** — analytical storage and query engine

### Execution Environment
- **Kubernetes** — Spark job execution environment

### Version Control
- **GitLab / Git** — source code management

---

## Data Flow

1. Raw data is stored in S3
2. Spark reads and processes the data
3. Processed data is written back to S3
4. Greenplum accesses data via external tables
5. Analytical views are created for reporting

---

## Pipeline Orchestration

The pipeline is implemented as an Airflow DAG consisting of:

1. **SparkKubernetesOperator**
   - запускает Spark job

2. **SparkKubernetesSensor**
   - отслеживает выполнение Spark job

3. **SQLExecuteQueryOperator**
   - создаёт внешние таблицы и аналитические представления в Greenplum
