from neo4jconnection import Neo4jConnection
from data_generator import DataGenerator
from dotenv import dotenv_values
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

config = dotenv_values(".env")
conn = Neo4jConnection(uri=config['uri'], user=config[], pwd="subham")
todaydate = str(datetime.now().date())
names = ['vinit','guilermo','christian','elly','subham']
brand = {'vinit':'samsung','guilermo':'nokia','christian':'redmi','elly':'moto','subham':'apple'}
os = {'samsung':'android','redmi':'android','moto':'windows','apple':'mac','nokia':'windows'}

def generate_data(name:str, date: str, os:str, brand: str):
    dg = DataGenerator(name,date,os,brand)
    return dg.add_data()

def get_present_time():
    time = str(datetime.now().time()).split('.')[0]
    return time

def create_user_label(name:str,data):
    query_string =  'CREATE ('+name+':User{idMaster: "'+ data['user_id'] +'"})'
    conn.query(query_string, db='demodb')

def create_app_label(index:int,data):
    query_string =  'CREATE ('+data['usages'][index]['app_name']+':App{idMaster: "'+ data['usages'][index]['app_name'] +'", AppCategory: "' + data['usages'][index]['app_category']+'"})'
    conn.query(query_string, db='demodb')

def create_device_label(device:str,data):
    query_string =  'CREATE ('+device+':Device{idMaster: "'+ data['device']['os'] +'"})'
    conn.query(query_string, db='demodb')

def create_brand_label(brand:str,data):
    query_string =  'CREATE ('+brand+':Brand{idMaster: "'+ data['device']['brand'] +'"})'
    conn.query(query_string, db='demodb')

def create_all_nodes(index:int,name:str,device:str, brand:str,data):
    create_user_label(name,data)
    create_device_label(device,data)
    create_brand_label(brand,data)
    create_app_label(index,data)

def create_user_relationship(index: int,data):
    query_string = '''MATCH (a:User), (b:App) WHERE a.idMaster = "'''+data['user_id']+'''" AND b.idMaster = "'''+data['usages'][index]['app_name']+'''" 
                      CREATE (a)-[r:USED {TimeCreated:"'''+get_present_time()+'''",TimeEvent:"'''+data['usage_date']+'''",UsageMinutes:"'''+str(data['usages'][index]['minutes_used'])+'''"}]->(b) '''
    conn.query(query_string, db='demodb')

def create_on_relationship(index:int,data):
    query_string='''Match (a:App), (b:Device) WHERE a.idMaster = "'''+data['usages'][index]['app_name']+'''" AND b.idMaster = "'''+data['device']['os']+'''"
                CREATE (a)-[r:ON {TimeCreated:"'''+get_present_time()+'''"}]->(b) '''
    conn.query(query_string,db='demodb')

def create_of_relationship(data):
    query_string='''Match (a:Device), (b:Brand) WHERE a.idMaster = "'''+data['device']['os']+'''" AND b.idMaster = "'''+data['device']['brand']+'''"
                CREATE (a)-[r:ON {TimeCreated:"'''+get_present_time()+'''"}]->(b) '''
    conn.query(query_string,db='demodb')

def create_all_relationships(index:int, data):
    create_user_relationship(index,data)
    create_on_relationship(index,data)
    create_of_relationship(data)


def generate_data_pipeline(date:str,**context):
    if date == None:
        date = todaydate
    data_lake=[]
    for name in names:
        data = generate_data(name=name,date=date,brand=brand[name], os=os[brand[name]])
        data_lake.append(data)
    context['ti'].xcom_push(key="data", value=data_lake)
    return "Data Generated Successfully"


def load_data_neo4j_pipeline(**context):
    data_lake = context.get("ti").xcom_pull(key="data")
    for data in data_lake:
        print(len(data['usages']))
        for index in range(0,len(data['usages'])):
            create_all_nodes(index=index,name=data['user_id'],device=data['device']['os'],brand=data['device']['brand'],data=data)
            create_all_relationships(index,data=data)
    return "Data Loaded Succesfully"

with DAG(
    dag_id="main",
    schedule_interval = "@daily",
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay": timedelta(minutes=2),
        "start_date": datetime(2021,4,1)
    },
    catchup=True) as f:

    generate_data_pipeline = PythonOperator(
        task_id="generate_data_pipeline",
        python_callable=generate_data_pipeline,
        op_args=[f.start_date],
        provide_context = True)
    load_data_neo4j_pipeline = PythonOperator(
        task_id="load_data_neo4j_pipeline",
        python_callable=load_data_neo4j_pipeline,
        provide_context = True)
    generate_data_pipeline >> load_data_neo4j_pipeline




