import os
import sqlalchemy
from ..config.config import Config

config = Config()
# using private ip and calling from environmental variable
db_user = config.getDB_user()
db_pass = config.getSecret()
db_name = config.getDB_name()
db_host = config.getDB_host()
db_driver = config.getDB_driver()

# creating a empty conn
conn = None

# Extracting host and port from db_host
host_args = db_host.split(":")
db_hostname, db_port = host_args[0], int(host_args[1])



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

def table_exists(engine,name):
    ins = sqlalchemy.inspect(engine)
    ret =ins.has_table(engine,name)
    print('Table "{}" exists: {}'.format(name, ret))
    return ret


def insert_raw_status(db_schema, table_name, table_field_1, table_field_2, table_field_3,table_field_4, table_field_value_1,table_field_value_2, table_field_value_3,table_field_value_4):
    localconn = getconn()
    hasTable = table_exists(localconn,table_name)
    stmtone = sqlalchemy.text('CREATE TABLE IF NOT EXISTS {} ('
               'id SERIAL UNIQUE,'
               'rawpath TEXT NOT NULL,'
               'processedpath TEXT, '
               'processed BOOLEAN, '
               'etl BOOLEAN,'
               'PRIMARY KEY (id));'.format(db_schema + '.' +table_name))
    stmttwo = sqlalchemy.text('insert into {} ({},{},{},{}) values ($${}$$,{},{},{})'.format(db_schema + '.' + table_name, table_field_1, table_field_2 , table_field_3, table_field_4, table_field_value_1, table_field_value_2, table_field_value_3,table_field_value_4))
    if hasTable is False:
        try:
             localconn.execute(stmtone)
             localconn.execute(stmttwo)
             print('success')
        except Exception as e:
            print(e)
        return 'ok'
    else:
        try:
             localconn.execute(stmttwo)
             print('success')
        except Exception as e:
            print(e)
        return 'ok'

def query_processed_status(db_schema,table_name, processed_column, processed_status):
    localconn = getconn()
    stmtone = sqlalchemy.text(' select rawpath from {} where {}={};'.format(db_schema+'.'+ table_name, processed_column,processed_status))
    rawpathlist = []

    try:
        localconn.execute(stmtone)

        for row in localconn.execute(stmtone).fetchall():
            rawpathlist.append(row)
        print('suceess',rawpathlist)
    except Exception as e:
            print(e)
    return rawpathlist