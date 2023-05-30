from google.cloud import secretmanager
import os


os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = r"service-account.json"
#values for testing offline
#os.environ["DB_USER"] = "halcyon-use"
#os.environ["DB_NAME"] = "halcyondatabase"
#os.environ["DB_HOST"] = "127.0.0.1:5432"
#os.environ["DB_DRIVER"] = "postgresql+pg8000"
#os.environ["project_id"] = "910286522764"
#os.environ["processor_id"] = '2e7febc7816e64e4'
#os.environ["gcs_output_bucket"] = '1040_processed'


# Setup the Secret manager Client
client = secretmanager.SecretManagerServiceClient()
# Get the sites environment credentials
project_id = os.environ.get('project_id')


#get secret
class Config(object):
    def getSecret(self):
        secret_name = "db_password"
        resource_name = f"projects/{project_id}/secrets/{secret_name}/versions/1"
        response = client.access_secret_version(resource_name)
        dbpassword = response.payload.data.decode('UTF-8')
        return dbpassword

    def getDB_user(self):
        db_user = os.environ.get('DB_USER')
        return db_user

    def getDB_name(self):
        db_user = os.environ.get('DB_NAME')
        return db_user


    def getDB_host(self):
        db_user = os.environ.get('DB_HOST')
        return db_user

    def getDB_driver(self):
        db_user = os.environ.get('DB_DRIVER')
        return db_user

    def getDB_user(self):
        db_user = os.environ.get('DB_USER')
        return db_user

    def getProccessId(self):
        process_id = os.environ.get('processor_id')
        return process_id

    def getGcsBucket(self):
        gsc_bucket = os.environ.get('gcs_output_bucket')
        return gsc_bucket

    def getProjectId(self):
        project_id = os.environ.get('project_id')
        return project_id