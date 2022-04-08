import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account

#CHANGE THIS
# Path to the json credential file
cred_key_path = 'C:\\Users\\Khoa\\Desktop\\python\\privatekey2.json'

# Google Account Credentials
credentials = service_account.Credentials.from_service_account_file(
    cred_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)

#CHANGE THIS
table_ref = 'flask-upload-345423.sample.email'
table_sch = []
try:
    SchemaJob = client.get_table(table_ref)
    print("Table Schema: {}".format(SchemaJob.schema))
    for s in SchemaJob.schema:
        new_dict = {}
        new_dict['name'] = s.name
        new_dict['type'] = s.field_type
        new_dict['mode'] = s.mode
        table_sch.append(new_dict)
    
    print(table_sch)
    #OUTPUT: [{'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'id', 'type': 'FLOAT', 'mode': 'NULLABLE'}]
except Exception as e:
    print(e)
        
   