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
    "start_date": datetime(2026, 7, 5),
    "retries": 1
}

dag = DAG(
    dag_id="full_geo_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

user_geo = SparkSubmitOperator(
    task_id="user_geo",
    dag=dag,
    conn_id="yarn_spark",
    application="/lessons/user_geo_job.py",
    py_files="/lessons/haversine.py,/lessons/city_matcher.py,/lessons/act_city.py,/lessons/home_city.py,/lessons/travel.py",
    application_args=[
        "2022-05-31",
        "/user/master/data/geo/events",
        "/user/master/data/geo/geo.csv",
        "/user/s27040058/data/analytics"
    ]
)
zone_geo = SparkSubmitOperator(
    task_id="zone_geo",
    dag=dag,
    conn_id="yarn_spark",
    application="/lessons/zone_geo_job.py",
    py_files="/lessons/city_matcher.py,/lessons/zone_event.py,/lessons/zone_mart.py,/lessons/haversine.py",
    application_args=[
        "2022-05-31",
        "/user/master/data/geo/events",
        "/user/master/data/geo/geo.csv",
        "/user/s27040058/data/analytics"
    ]
)

friends = SparkSubmitOperator(
    task_id="friend_recommendations",
    dag=dag,
    conn_id="yarn_spark",
    application="/lessons/friend_recommendations.py",
    py_files="/lessons/city_matcher.py,/lessons/haversine.py",
    application_args=[
        "2022-05-31",
        "/user/master/data/geo/events",
        "/user/master/data/geo/geo.csv",
        "/user/s27040058/data/analytics"
    ]
)

user_geo >> zone_geo >> friends