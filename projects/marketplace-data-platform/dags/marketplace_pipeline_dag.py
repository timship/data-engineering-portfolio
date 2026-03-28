import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"

SUBMIT_NAME = "job_submit"


def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag
    )


def _build_sensor(task_id: str, application_name: str, link_dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=link_dag
    )


create_table_sql = """
        DROP EXTERNAL TABLE IF EXISTS "marija-shkurat-wrn7887".seller_items CASCADE;

        CREATE EXTERNAL TABLE "marija-shkurat-wrn7887".seller_items (
        sku_id BIGINT,
        title TEXT,
        category TEXT,
        brand TEXT,
        seller TEXT,
        group_type TEXT,
        country TEXT,
        availability_items_count BIGINT,
        ordered_items_count BIGINT,
        warehouses_count BIGINT,
        item_price BIGINT,
        goods_sold_count BIGINT,
        item_rate FLOAT8,
        days_on_sell BIGINT,
        avg_percent_to_sold BIGINT,
        returned_items_count INTEGER,
        potential_revenue BIGINT,
        total_revenue BIGINT,
        avg_daily_sales FLOAT8,
        days_to_sold FLOAT8,
        item_rate_percent FLOAT8
        ) LOCATION ('pxf://startde-project/marija-shkurat-wrn7887/seller_items?PROFILE=s3:parquet&SERVER=default')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
"""

create_unreliable_sellers_report_view_sql = """
CREATE OR REPLACE VIEW "marija-shkurat-wrn7887".unreliable_sellers_view AS
SELECT  
    seller,
    SUM(availability_items_count) AS total_overload_items_count,
    CASE WHEN (AVG(days_on_sell) > 100 AND SUM(availability_items_count) > SUM(ordered_items_count)) 
         THEN TRUE 
         ELSE FALSE 
         END AS is_unreliable
    FROM "marija-shkurat-wrn7887".seller_items
    GROUP BY seller;
"""

create_brands_report_view_sql = """
CREATE OR REPLACE VIEW "marija-shkurat-wrn7887".item_brands_view AS
SELECT  
    brand,
    group_type,
    country,
    CAST(SUM(potential_revenue) AS FLOAT8) AS potential_revenue,
    CAST(SUM(total_revenue)     AS FLOAT8) AS total_revenue,
    CAST(COUNT(sku_id)          AS BIGINT) AS items_count
FROM "marija-shkurat-wrn7887".seller_items
GROUP BY brand, group_type, country;
"""

with DAG(
    dag_id="startde-project-marija-shkurat-wrn7887-dag",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 9, 10, tz="UTC"),
    tags=["final_project", "marija-shkurat-wrn7887"],
    catchup=False
) as dag:

    submit_task = _build_submit_operator(
        task_id=SUBMIT_NAME,
        application_file='spark_submit.yaml',
        link_dag=dag
    )

    sensor_task = _build_sensor(
        task_id='job_sensor',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME}')['metadata']['name']}}}}",
        link_dag=dag
    )

    items_datamart = SQLExecuteQueryOperator(
        task_id='items_datamart',
        conn_id=GREENPLUM_ID,
        sql=create_table_sql,
        split_statements=True,
        return_last=False,
    )

    create_unreliable_sellers_report_view = SQLExecuteQueryOperator(
        task_id='create_unreliable_sellers_report_view',
        conn_id=GREENPLUM_ID,
        sql=create_unreliable_sellers_report_view_sql,
        split_statements=True,
        return_last=False,
    )

    create_brands_report_view = SQLExecuteQueryOperator(
        task_id='create_brands_report_view',
        conn_id=GREENPLUM_ID,
        sql=create_brands_report_view_sql,
        split_statements=True,
        return_last=False,
    )



    submit_task >> sensor_task >> items_datamart >> [create_unreliable_sellers_report_view, create_brands_report_view, ]
