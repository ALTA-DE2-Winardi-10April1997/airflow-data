from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import requests

@dag(
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['api', 'gender-api-v2']
)
def api_example():

    @task(task_id="get_data_gender_api")
    def get_data_api():
        myKey = "c17e6f1f6cd496b52d441764789c621c80fba63d64113f8e5b3001c73c792710"
        url = "https://gender-api.com/get?key=" + myKey + "&name=sandra"
        response = requests.get(url)
        data = response.json()
        print("Gender: " + data["gender"])  # Gender: male
        return data

    @task(task_id="load_to_postgres")
    def load_to_postgres(data):
        pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
        
        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS gender_name_prediction (
            id SERIAL PRIMARY KEY,
            input JSONB,
            details JSONB,
            result_found BOOLEAN,
            first_name VARCHAR(100),
            probability FLOAT,
            gender VARCHAR(50),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        pg_hook.run(create_table_query)

        # Prepare data for insertion
        record = (
            json.dumps({"first_name": data["name"], "country": data.get("country", "unknown")}),
            json.dumps(data),
            data.get("result_found", True),
            data.get("name"),
            data.get("accuracy", 1.0),
            data.get("gender")
        )
        
        insert_query = """
        INSERT INTO gender_name_prediction (input, details, result_found, first_name, probability, gender)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        pg_hook.run(insert_query, parameters=record)

    data = get_data_api()
    load_to_postgres(data)

api_example()
