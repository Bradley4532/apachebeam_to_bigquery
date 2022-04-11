import os
import pandas as pd
import numpy as np
import json
from flask import Flask, render_template, request
from pandas import json_normalize
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
import re

def get_error_report(errors, file_dataframe):
    report = ""
    try:
        for error_index, error in enumerate(errors):
            try:
                error_value = re.findall("'\D*'", error['message'])[0][1:-1]
                found_error_records = file_dataframe[file_dataframe.isin([error_value]).any(axis=1)].values #doesnt find null errors, needs to be fixed--------------------
            except:
                found_error_records = 'IDK'
            log_string = """Error #{} || Error Message - {}""".format(error_index, error['message'])
            report+= log_string + "\n\n"
    except:
        raise
    return str(report)
# Path to the json credential file
cred_key_path = 'C:\\Users\\Khoa\\Desktop\\python\\privatekey2.json'

# Google Account Credentials
credentials = service_account.Credentials.from_service_account_file(
    cred_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)
dataset_name = "sample"
table_ref = 'flask-upload-345423.sample.email'

#Flask
app = Flask(__name__)

#Path to project directory
APP_ROOT = "C:\\Users\\Khoa\\Documents\\Python Scripts\\FlaskUpload"

@app.route('/')
def index():
    return render_template("upload_page.html")
    
@app.route('/upload_page', methods = ['POST'])
def upload():
    target = os.path.join(APP_ROOT, 'files/')

    info = ""
    #Creates the directory that holds the files
    if not os.path.isdir(target):
        os.mkdir(target)
    
    #Goes thru all files
    for file in request.files.getlist("file"):
        print(file)
        filename = file.filename
        print("File Name: ", filename)
        destination = "/".join([target, filename])
        print("destination:", destination)

        #Checks to see if the file is a JSON file, if so error message on front end
        if not destination.endswith(".json"):
            info = "ERROR: Please enter a JSON file"
            return render_template("upload_page.html", info=info)
        
        #Saves file to destination
        file.save(destination)

        #Make Pandas dataframe from JSON ---------------------------------------------------------
        fileLoc = "files/" + filename
        print(fileLoc)

        #Open JSON file
        with open('files/email.json') as json_file:
            err_msg = ""
            data = json.load(json_file)
            df = json_normalize(data)
            try:
                print("START")
                job_config = bigquery.job.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("name", "STRING","REQUIRED"),
                    bigquery.SchemaField("email", "STRING","REQUIRED"),
                    bigquery.SchemaField("id", "INT64","REQUIRED")
                ],
                max_bad_records=1000,

                )

                # Overwrite current table contents
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                job_config.source_format = bigquery.SourceFormat.CSV

                load_job = client.load_table_from_dataframe(
                        df,
                        table_ref,
                        job_config=job_config
                )
                print("CREATE TABLE SUCCESS")

                load_job.result()
                if load_job.errors:
                    log = get_error_report(load_job.errors,df)
                    print(log)
                    err_file = open("files/ERROR.txt", "w")
                    err_file.write(log)
                    err_file.close()
            except Exception as e:
                print(e)
                print("ERROR")
            print("done")


        return render_template("home.html")



if __name__=="__main__":
    app.run()