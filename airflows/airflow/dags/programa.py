import json
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd;
@dag(
    schedule='*/5 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pievi"],
)
def tutorial_aula_pie():
    @task()
    def extract():
        res = requests.get('https://jsonplaceholder.typicode.com/comments')
        vcomments = res.json()
        print(vcomments)
        return vcomments
    
    @task()
    def createTable():
        query = """
            drop table comments;
            create table comments (
                postId int
                , id int
                , name varchar(255)
                , email  varchar(255)
                , body text
            );
        """
        hook = PostgresHook(postgres_conn_id='dwteste')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()

    @task()
    def load_raw(vcomments):
        hook = PostgresHook(postgres_conn_id='dwteste',)
        conn = hook.get_conn()
        cur = conn.cursor()

        df = pd.DataFrame.from_dict(vcomments)
        for item in df.to_dict(orient="records"):   
            query = f""" 
            INSERT INTO public."comments"
            (postid, id, "name", email, body)
            VALUES({item['postId']}, {item['id']}, '{item['name']}', '{item['email']}', '{item['body']}');
            """
            print(query)
            cur.execute(query)
        conn.commit()

    extract1 = extract()
    createTable1 = createTable()
    load_raw1= load_raw(extract1)
    
    createTable1 >> extract1 >> load_raw1

tutorial_aula_pie()