import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 7, 5)
}

dag = DAG(
    dag_id='user_geo_dag',
    default_args=default_args,
    schedule_interval=None
)

user_geo = SparkSubmitOperator(
    task_id='user_geo',
    dag=dag,
    application='/lessons/user_geo_job.py',
    conn_id='yarn_spark',

    py_files=','.join([
        '/lessons/haversine.py',
        '/lessons/city_matcher.py',
        '/lessons/act_city.py',
        '/lessons/home_city.py',
        '/lessons/travel.py'
    ]),

    application_args=[
        '2022-05-31',
        '/user/master/data/geo/events',
        '/user/master/data/geo/geo.csv',
        '/user/s27040058/data/analytics'
    ]
)