import json
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.hooks import PostgresHook
import pandas as pd;
@dag(
    schedule='*/5 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pievi"],
)
def tutorial_etl_table_cicle():
    def insertTable(vcomments):
        hook = PostgresHook(postgres_conn_id='dwteste',)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute('delete from stage."comments" where 1=1;')
        df = pd.DataFrame.from_dict(vcomments)
        for item in df.to_dict(orient="records"):   
            query = f""" 
            INSERT INTO stage."comments"
            (postid, id, "name", email, body, load_date)
            VALUES({item['postId']}, {item['id']}, '{item['name']}', '{item['email']}', '{item['body']}', now());
            """
            cur.execute(query)
        conn.commit()

    def insertTablePosts(vposts):
        hook = PostgresHook(postgres_conn_id='dwteste',)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute('delete from stage.tblPost where 1=1;')
        df = pd.DataFrame.from_dict(vposts)
        for item in df.to_dict(orient="records"):   
            query = f""" 
            INSERT INTO stage.tblPost
            (postid, id, "name", email, body, load_date)
            VALUES({item['postId']}, {item['id']}, '{item['name']}', '{item['email']}', '{item['body']}', now());
            """
            cur.execute(query)
        conn.commit()
    
    @task()
    def extract_comments():
        res = requests.get('https://jsonplaceholder.typicode.com/comments')
        vcomments = res.json()
        insertTable(vcomments)
        return vcomments
    
    @task()
    def extract_users():
        res = requests.get('https://jsonplaceholder.typicode.com/comments')
        vcomments = res.json()
        ## insertTable(vcomments)
        return vcomments
    
    @task()
    def extract_posts():
        res = requests.get('https://jsonplaceholder.typicode.com/posts')
        vposts = res.json()
        insertTablePosts(vposts)
        return vposts
    
    @task()
    def load():
        hook = PostgresHook(postgres_conn_id='dwteste')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("""delete from public."comments" 
            where id in (select id from stage."comments") """)
        
        cur.execute("""insert into public."comments" 
                       select * from stage."comments" """)
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

    @task
    def transformer():
        ## delete todos os post do usuario 1
        ...

    @task
    def transformer2():
        ## delete todos os post do usuario 1
        ...

    t_extract_comments = extract_comments()
    t_extract_posts = extract_posts()
    t_extract_users = extract_users()
    t_tranfomer = transformer()
    load1 = load()
    #createTable1 = createTable()
    #load_raw1= load_raw(extract1)
    
    #createTable1 >> extract1 >> load_raw1
    [t_extract_comments, t_extract_posts, t_extract_users] >> t_tranfomer >> load1 
     # >> load1
tutorial_etl_table_cicle()