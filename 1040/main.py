from sqlfunctions import insert_raw_status, query_processed_status
from docai import batch_process_documents
from config import Config

config = Config()


def main(event,context):
    
    ## local vars ##
    
    bucket = event["bucket"]
    fileName = event["name"]
    gcs_input_uri = "gs://"+bucket+"/"+fileName
    x = gcs_input_uri.split("/")
    file_path = x[3:6]
    print(file_path)
    gcs_output_bucket = config.getGcsBucket()

    gcs_output_uri = "gs://"+ gcs_output_bucket
    project_id = config.getProjectId()

    location = 'us'
    processor_id = config.getProccessId()
        
    # variables for to be written to postgres instance on cloud sql
    split_bucket = bucket.split('_')
    split_bucket = split_bucket[0].replace("-","_")
    schema = 'documentstate'
    table_name = 'parser_' + split_bucket
    table_field_1 = "rawpath"
    table_field_2 = "processedpath"
    table_field_3 = 'processed'
    table_field_4 = 'etl'
    table_field_value_1 = gcs_input_uri
    table_field_value_2 = 'null'
    table_field_value_3 = False
    table_field_value_4 = False
    insert_raw_status(schema,table_name, table_field_1,table_field_2 , table_field_3,table_field_4, table_field_value_1, table_field_value_2, table_field_value_3,table_field_value_4)

    raw_documents = query_processed_status(schema,table_name,table_field_3,table_field_value_3)
    bloblist = raw_documents
    for i in bloblist:
        blob_path = str(i)
        formatted_path = blob_path.replace('(','')
        formatted_path = formatted_path.replace(')','')
        formatted_path = formatted_path.replace(',','')
        gcs_input_path = formatted_path.replace("'",'')
        print(gcs_input_path)
        split_path = formatted_path.split('/')
        output_path_prefix = split_path[3] + '/' + split_path[4] + '/' + split_path[5] 
        print(output_path_prefix)
    
        batch_process_documents(project_id,location,processor_id,gcs_input_path,gcs_output_uri,output_path_prefix)

    return "documents have been processed"



