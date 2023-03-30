from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

import psycopg2 as postgre
import pyodbc as mssql
import urllib.request as req
import ssl


def _get_data_from_site(ti):
    f = req.urlopen("https://meteo.gov.ua/ua/33345/hydrology/hydr_water_level_changes_map/")
    str = f.read().decode('utf-8')
    str = str[str.find("L.tileLayer"):]
    inf = str[str.find("var string"):]
    inf = inf[inf.find('"') + 1:inf.find(";") - 1]
    coord = str[str.find("var string2"):]
    coord = coord[coord.find('"') + 1:coord.find(";") - 1]
    
    ti.xcom_push(key='inf', value=inf)
    ti.xcom_push(key='coord', value=coord)


def _parse(ti):
    s1= ti.xcom_pull(key='inf', task_ids='get_data_from_site')
    s2= ti.xcom_pull(key='coord', task_ids='get_data_from_site')
    
    geo_dict = {}
    for j in s2.split("*"):
        if j != "":
            p_geo = j.split("/")
            geo_dict[p_geo[0]] = (p_geo[1], p_geo[2])

    post = []
    for i in s1.split("*"):
        if i != "":
            pxy = i.split("/")
            post.append({"River": pxy[0], "City": pxy[2], "Region": pxy[3].rstrip(), "Water level": pxy[4],
                         "Baltic height system": pxy[5], "Difference": pxy[6], "Yesterday WL": pxy[7],
                         "-2 WL": pxy[8], "-3 WL": pxy[9], "Temperature": pxy[10], "X": geo_dict[pxy[1]][0], "Y": geo_dict[pxy[1]][1]})

    for i in post:
        if i["Water level"] != "":
            i["Water level"] = round(float(i["Water level"]), 2)

        if i["Baltic height system"] != "":
            i["Baltic height system"] = round(float(i["Baltic height system"]), 2)

        if i["Difference"] != "":
            i["Difference"] = round(float(i["Difference"]), 2)

        if i["Yesterday WL"] != "":
            i["Yesterday WL"] = round(float(i["Yesterday WL"]), 2)
        
        if i["-2 WL"] != "":
            i["-2 WL"] = round(float(i["-2 WL"]), 2)
            
        if i["-3 WL"] != "":
            i["-3 WL"] = round(float(i["-3 WL"]), 2)

        if i["Temperature"] != "":
            i["Temperature"] = round(float(i["Temperature"]), 2)

        if i["X"] != "":
            i["X"] = round(float(i["X"]), 5)
        
        if i["Y"] != "":
            i["Y"] = round(float(i["Y"]), 5)

        i["River"] = i["River"].replace("'", "''")
        i["City"] = i["City"].replace("'", "''")
        i["Region"] = i["Region"].replace("'", "''")
    
    ti.xcom_push(key='post', value=post)    

def _insert_to_db_postgreSQL(ti):
    data= ti.xcom_pull(key='post', task_ids='parse')
    pg_hook=PostgresHook(postgres_conn_id="airflow_postgres", schema="riverinfo")
  
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""SELECT CURRENT_DATE""")
    curr_date = cursor.fetchone()[0]

    for i in data:
        if i["X"] == "" or i["Y"] == "":
            continue

        cursor.execute("""SELECT post_id FROM posts WHERE x = '%s' AND y = '%s'"""%(str(i["X"]), str(i["Y"])))
        postID = cursor.fetchone()

        if postID is None:
            cursor.execute("""INSERT INTO posts(river_name, city, region, x, y) VALUES ('%s','%s','%s','%s','%s')"""
                            %(i["River"], i["City"], i["Region"], str(i["X"]), str(i["Y"])))

            cursor.execute("""SELECT post_id FROM posts WHERE x = '%s' AND y = '%s'"""%(str(i["X"]), str(i["Y"])))
            postID = cursor.fetchone()

        cursor.execute("""SELECT info_id FROM sample_info WHERE post_id = %f AND test_date = '%s'"""%(postID[0], curr_date))
        infoID = cursor.fetchone()

        if infoID is not None:
            cursor.execute("""DELETE FROM sample_info WHERE info_id = %f"""%(infoID[0]))


        cursor.execute("""INSERT INTO sample_info(post_id, test_date, water_level, bhs, temperature) VALUES (%f,'%s','%s','%s','%s')"""
                    %(postID[0], curr_date, str(i["Water level"]), str(i["Baltic height system"]), str(i["Temperature"])))
    conn.commit()


with DAG("WF_DWH_RIVERINFO_TO_POSTGRE",
    start_date=datetime(2021, 12 ,21),
    schedule_interval='8 0 * * *',
    catchup=False) as dag:

    ssl._create_default_https_context = ssl._create_unverified_context
    
    get_data_from_site = PythonOperator(
        task_id="get_data_from_site",
        python_callable=_get_data_from_site
    )

    parse = PythonOperator(
        task_id="parse",
        python_callable=_parse
    )

    insert_to_db_postgreSQL = PythonOperator(
        task_id="insert_to_db_postgreSQL",
        python_callable=_insert_to_db_postgreSQL
    )



get_data_from_site >> parse >> insert_to_db_postgreSQL
