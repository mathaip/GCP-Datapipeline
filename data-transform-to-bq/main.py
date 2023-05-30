import pandas as pd
import json
import os
from google.cloud import bigquery, storage
from google.api_core import retry
import base64
import sqlalchemy
from google.cloud import secretmanager

os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = r"service-account.json"


BQ = bigquery.Client()
client = secretmanager.SecretManagerServiceClient()

# using private ip and calling from environmental variable
db_user = os.environ.get('DB_USER')
db_name = os.environ.get('DB_NAME')
db_host = os.environ.get('DB_HOST')
db_driver = os.environ.get('DB_DRIVER')
project_id = os.environ.get('project_id')

secret_name = "db_password"
resource_name = f"projects/{project_id}/secrets/{secret_name}/versions/1"
response = client.access_secret_version(resource_name)
dbpassword = response.payload.data.decode('UTF-8')
db_pass = dbpassword

conn = None

# Extract host and port from db_host
host_args = db_host.split(":")
db_hostname, db_port = host_args[0], int(host_args[1])



PROJECT_ID = 'halcyonsw'
storage_client = storage.Client()




def main(event, context):
    
    ## Read from all tables in documentstate schema and find processedpath column where processed = true  
    schema = 'documentstate'
    column = 'processedpath'  
    tables = get_all_tables(schema,column)
    for table in tables:
        table_name = schema + '.' + table
        processed_documents = query_status(table_name)
        bloblist = processed_documents
        for i in bloblist:
            blob_path = str(i)
            ### Cleaning the gcs document path of the extra string elements from cloud sql
            formatted_path = blob_path.replace('(','')
            formatted_path = formatted_path.replace(')','')
            formatted_path = formatted_path.replace(',','')
            ### END ###
            
            split_path = formatted_path.split('/')
            bucket_name = split_path[2]
            blob_path = split_path[3] + '/' + split_path[4] + '/' + split_path[5] + '/' + split_path[6] + '/' + split_path[7]+ '/' + split_path[8]
            blob_path = blob_path.replace("'","")
            split_bucket_name = bucket_name.split('_')
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            orgColumn = 'organization'
            personColumn = 'person'
            organization = split_path[3]
            person = split_path[4]
            
            
            ##table name for big query##
            bq_table_name = split_bucket_name[0]
            ### END ###
            
            ### variables for update cloud sql etl status ###
            processed_path = formatted_path
            etl_state = True
            processed_state = True
            ###  END ###

            ### Data transformation Section using pandas dataframes
            processed_document = blob.download_as_string(client=None)
            loadedjson = json.loads(processed_document)
            
            df = pd.json_normalize(loadedjson['entities'])
        
            df[orgColumn] = organization
            df[personColumn] = person
            df.pop('pageAnchor.pageRefs')
            df.pop('textAnchor.content')
            confidence_df = df.copy()
            confidence_pivot = confidence_df.pivot(index=['organization','person'], columns=['type'],values='confidence')
            confidence_type = confidence_pivot.add_suffix('_confidence')
            pivot = df.pivot(index=['organization','person'],columns=['type'],values='mentionText')
            merged_df = pivot.join(confidence_type, on=['organization','person'])
            ### END ###
            

            ### Check if Table exists and load data from dataframe into bigquery
            dataset = BQ.dataset("documents")

            table_ref = dataset.table(bq_table_name)
        
            table_exists = doesTableExist(table_ref)
            
            if table_exists == False:
               print('creating table...')
               table = bigquery.Table(table_ref)

               table = BQ.create_table(table) 
            
            job = BQ.load_table_from_dataframe(
                merged_df, table_ref
                )  # Make an API request.
            job.result()
            table = BQ.get_table(table_ref)  # Make an API request.
            print("Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_ref))
            ### END ###
            
            ### Update CLOUD SQL Etl state for Documents that have been inserted into BigQuery
            res = update_status(table_name,etl_state,processed_path, processed_state)
            print(res)
            ### END ###
    
    
def getconn():
    global conn
    if conn is None:
         conn = sqlalchemy.create_engine(
    sqlalchemy.engine.url.URL.create(
        drivername=db_driver,
        username=db_user,  
        password=db_pass,  
        host=db_hostname,  
        port=db_port,  
        database=db_name  
        ),
            
    )
         conn.connect()
            
    return conn

def query_status(table_name):
    localconn = getconn()
    stmtone = sqlalchemy.text(' select processedpath from {} where processed=true and etl=false;'.format(table_name))
    processedpathlist = []

    try:
        localconn.execute(stmtone)

        for row in localconn.execute(stmtone).fetchall():
            print(row)
            processedpathlist.append(row)
        print('suceess',processedpathlist)
    except Exception as e:
            print(e)
    return processedpathlist

def update_status(table_name,etl_state,processed_path, processed_state):
    localconn = getconn()
    s = ""
    s += "UPDATE {}".format(table_name)
    s += " SET etl={}".format(etl_state)
    s += " WHERE"
    s += " processedpath={}".format(processed_path)
    s += " AND"
    s += " processed={};".format(processed_state)

    try:
        print(s)
        localconn.execute(s)

        print('suceess etl set to True')
    except Exception as e:
            print(e)
    return "update etl done"


def doesTableExist(table_ref):
     try:
        BQ.get_table(table_ref)
        return True
     except Exception as err:
        if err: 
            return False

def get_all_tables(db_schema,processedpath_column):
    localconn = getconn()
    s = ""
    s += "SELECT"
    s += " table_name"
    s += " FROM information_schema.tables"
    s += " WHERE"
    s += " ("
    s += " table_schema = 'documentstate'"
    s += " )"
    s += " ORDER BY table_schema, table_name;"
    table_list =[]
    try:
        localconn.execute(s)
        list_tables = localconn.execute(s).fetchall()
        for i in list_tables:
            table_name = str(i)
            formatted_table_name = table_name.replace('(','')
            formatted_table_name = formatted_table_name.replace(')','')
            formatted_table_name = formatted_table_name.replace(',','')
            formatted_table_name = formatted_table_name.replace("'",'')
            table_list.append(formatted_table_name)
        print('success',table_list)
    except Exception as e:
            print(e)
    return table_list


