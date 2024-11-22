from airflow import DAG
#from airflow.providers.amazon.aws.transfers.s3 import S3FileTransferOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from airflow.models import XCom
import pandas as pd
import snowflake.connector
import boto3
import json
import csv
import io
from dotenv import load_dotenv
import os
load_dotenv()

dag = DAG(
    'crypto_to_s3_warehouse_to_analysis',
    description='DAG crypto',
    schedule_interval='@daily',  # S'exécute tous les jours (on dirait .... )
    start_date=datetime(2024, 11, 14),
    catchup=False,
)

def fetch_and_upload_data(**kwargs):
    url = lambda x : "https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol="+x+"&market=EUR&apikey="+os.getenv('API_KEY')
    # get the current configuration credentials
    session = boto3.Session(profile_name="default")
    credentials = session.get_credentials()
    s3 = session.client(
        's3',
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        region_name='eu-west-3'
    )
    bucket_name = 'youssef-crypto-data'
    # get the money lables : ex : BTC
    file_key = "p_crypto_keys/top_5_cryptocurrencies.csv"
    crypto_keys = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = crypto_keys['Body'].read().decode('utf-8')  # Convertir en chaîne UTF-8
    csv_reader = csv.reader(io.StringIO(file_content))
    i =0
    print(f"Checkpoint Retrieved start collection")
    for row in csv_reader:
        i=i+1
        response = requests.get(url(row[0]))
        data = response.json()
        if not "Time Series (Digital Currency Daily)" in data :
            pass
        else :
            print("Checkpoint Retrieved start collection ", i)
            data_string = json.dumps(data)
            s3_file_path = row[1]+'.json'
            #put objects without saving it 
            s3.put_object(Body=data_string, Bucket=bucket_name, Key=s3_file_path)
    
# step 1 : get the data from the api to s3
fetch_data = PythonOperator(
    task_id='fetch_data_from_api_to_S3',
    python_callable=fetch_and_upload_data,
    provide_context=True,
    dag=dag,
)

def transform(**kwargs):
    list_df = []
    session = boto3.Session(profile_name="default")
    credentials = session.get_credentials()
    s3 = session.client(
        's3',
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        region_name='eu-west-3'
    )
    bucket_name = 'youssef-crypto-data'
    response = s3.list_objects_v2(Bucket=bucket_name)
    print(response['Contents'])
    i = 0
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.json') :
                i = i + 1
                file = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                json_data = file['Body'].read().decode('utf-8')
                
                # Convert the JSON string to a Python object (list or dict)
                data = json.loads(json_data)
                
                # Traiter les données et les convertir en DataFrame Pandas
                time_series = data["Time Series (Digital Currency Daily)"]

                # Convertir les données en DataFrame en utilisant les dates comme index
                df = pd.DataFrame.from_dict(time_series, orient='index')

                # Renommer les colonnes pour plus de clarté
                df.columns = ['open', 'high', 'low', 'close', 'volume']

                # Convertir l'index (les dates) en une colonne
                df['date'] = df.index

                # Convertir la colonne 'date' en format datetime
                df['date'] = pd.to_datetime(df['date'])

                # Réorganiser les colonnes
                df = df[['date', 'open', 'high', 'low', 'close', 'volume']]

                # Convertir les colonnes numériques en float (optionnel, mais souvent utile pour les analyses)
                df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric)
                df.attrs['nom_table'] = data["Meta Data"]["3. Digital Currency Name"] + "_to_" +data["Meta Data"]["5. Market Name"]
                list_df.append(df)
    else:
        raise "No objects found in the S3 bucket."
    # Read the JSON content from the response
    print(len(list_df))
    list_df_serialized = [ {"data": df, "attrs": df.attrs['nom_table']} for df in list_df ]
    kwargs['ti'].xcom_push(key='buffer_data', value=list_df_serialized)
    

#step 2 transform data 
transform_data = PythonOperator(
    task_id='to_csv_columns',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

def load_to_data_wh(**kwargs):
    ti = kwargs['ti']
    data_list = ti.xcom_pull(task_ids='to_csv_columns', key='buffer_data')
    

    
    conn = snowflake.connector.connect(
        user=os.getenv('SNFK_ACC'),
        password=os.getenv('SNFK_PASSWORD'),
        account=os.getenv('SNFK_ACCOUNT'),
        database='crypto',
        schema='CRYPTO_SCHEMA'
    )

    cursor = conn.cursor()

    for item in data_list:
        df = item['data']
        name = item['attrs']
        print(df.describe())
        print('DROP TABLE IF EXISTS '+ name)
        cursor.execute('DROP TABLE IF EXISTS '+ name)
        cursor.execute('CREATE TABLE '+name+' ( date DATE, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume FLOAT )')

        # Insérer les données dans Snowflake
        for index, row in df.iterrows():
            # Convertir la date en chaîne de caractères au format 'YYYY-MM-DD'
            date_str = row['date'].strftime('%Y-%m-%d')
            
            cursor.execute("""
                INSERT INTO """+name+""" (date, open, high, low, close, volume)
                VALUES (TO_DATE(%s, 'YYYY-MM-DD'), %s, %s, %s, %s, %s)
            """, (date_str, row['open'], row['high'], row['low'], row['close'], row['volume']))

    # Commit et fermeture de la connexion
    conn.commit()
    cursor.close()
    conn.close()

#step 3
load_date = PythonOperator(
    task_id='load_new_data',
    python_callable=load_to_data_wh,
    provide_context=True,
    dag=dag,
)


fetch_data >> transform_data >> load_date