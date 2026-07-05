import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 6, 28)
}

dag = DAG(
    dag_id="zone_geo_job",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

zone_geo_task = SparkSubmitOperator(
    task_id="zone_geo_job_run",
    dag=dag,
    conn_id="yarn_spark",
    application="/lessons/zone_geo_job.py",
    py_files="/lessons/city_matcher.py,/lessons/zone_event.py,/lessons/zone_mart.py,/lessons/haversine.py",
    application_args=[
        "2022-05-31",
        "/user/master/data/geo/events",
        "/user/master/data/geo/geo.csv",
        "/user/s27040058/data/analytics/zone_mart"
    ]
)

zone_geo_task