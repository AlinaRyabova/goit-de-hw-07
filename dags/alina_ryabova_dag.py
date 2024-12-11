from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.state import State
import random
import time
from airflow.utils.dates import days_ago

# Функція для випадкового вибору типу медалі
def random_medal_choice():
    return random.choice(["Gold", "Silver", "Bronze"])

# Функція для імітації затримки
def delay_execution():
    time.sleep(35)

# Базові параметри DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

mysql_connection_id = "mysql_connection_alina"

with DAG(
    "alina_ryabova_dag2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["alina_medal_counting2"],
) as dag:

    create_table_task = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_connection_id,
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.alina_ryabova_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            medal_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    select_medal_task = PythonOperator(
        task_id="select_medal",
        python_callable=random_medal_choice,
    )

    def branching_logic(**kwargs):
        selected_medal = kwargs["ti"].xcom_pull(task_ids="select_medal")
        if selected_medal == "Gold":
            return "count_gold_medals"
        elif selected_medal == "Silver":
            return "count_silver_medals"
        else:
            return "count_bronze_medals"

    branching_task = BranchPythonOperator(
        task_id="branch_based_on_medal",
        python_callable=branching_logic,
        provide_context=True,
    )

    count_bronze_task = MySqlOperator(
        task_id="count_bronze_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.alina_ryabova_medal_counts (medal_type, medal_count)
           SELECT 'Bronze', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Bronze';
           """,
    )

    count_silver_task = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.alina_ryabova_medal_counts (medal_type, medal_count)
           SELECT 'Silver', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Silver';
           """,
    )

    count_gold_task = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.alina_ryabova_medal_counts (medal_type, medal_count)
           SELECT 'Gold', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Gold';
           """,
    )

    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_last_record_task = SqlSensor(
        task_id="verify_recent_record",
        conn_id=mysql_connection_id,
        sql="""
            SELECT COUNT(*) > 0
            FROM neo_data.alina_ryabova_medal_counts
            WHERE created_at >= NOW() - INTERVAL 30 SECOND;
        """,
        mode="poke",
        poke_interval=10,
        timeout=60,
    )

    create_table_task >> select_medal_task >> branching_task
    (
        branching_task
        >> [count_bronze_task, count_silver_task, count_gold_task]
        >> delay_task
    )
    delay_task >> check_last_record_task
