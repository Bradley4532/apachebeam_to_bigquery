#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().system(' pip install flask')
get_ipython().system(' pip install --upgrade google-cloud-bigquery')
get_ipython().system(' pip install pyarrow')


# In[1]:


from flask import Flask,render_template,request, redirect, url_for, make_response
from google.cloud import bigquery, storage
from werkzeug.utils import secure_filename
import os
import pandas
import pyarrow
import json
import pandas as pd
import re
import numpy as np

pd.set_option('mode.use_inf_as_na', True)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'lucid-timing-343502-0c36003d6940.json'

UPLOAD_FOLDER_PATH = r'C:\Users\Hongquy\Documents\BigQueryFlask Application\UploadFiles'
TEMP_FOLDER_PATH = r'C:\Users\Hongquy\Documents\BigQueryFlask Application\TempFiles'
bucketPath = 'bigquery-flask-bucket'

EXTENSIONS = {'json', 'csv'}

datasetId = 'User'

client = bigquery.Client()
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER_PATH

def checkFile(filename):
    return '.' in filename and            filename.rsplit('.', 1)[1].lower() in EXTENSIONS

def getFiles(target):
    fileArray = []
    for file in os.listdir(target):
        path = os.path.join(target, file)
        if os.path.isfile(path):
            fileArray.append(file)
    return fileArray

def insertBQGCS(fileFormat, fileName):
    
    datasetRef = client.dataset(datasetId)
    tableId = fileName.rsplit('.', 1)[0].upper()
    tableRef = datasetRef.table(tableId)
    path = os.path.join(app.config['UPLOAD_FOLDER'], fileName) 
    missingFileName = fileName.rsplit('.', 1)[0] + "_Missing"
    
    if(fileFormat == 'json'):
        #jobConfig = bigquery.LoadJobConfig(schema=[
        #    bigquery.SchemaField("name", "STRING", "Required"),
        #    bigquery.SchemaField("job", "STRING", "Required"),
        #    bigquery.SchemaField("salary", "INT64", "Required"),
        #], max_bad_records=1000)
        jobConfig = bigquery.LoadJobConfig(max_bad_records = 1000)
        jobConfig.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        jobConfig.write_disposition = 'WRITE_TRUNCATE' 
        df = pd.read_json(path, lines=True, orient = "records")
    else:
        #jobConfig = bigquery.LoadJobConfig(schema=[
        #    bigquery.SchemaField("Key", "INTEGER", "Required"),
        #    bigquery.SchemaField("Value", "INTEGER", "Required"),
        #], max_bad_records=1000)
        
        jobConfig = bigquery.LoadJobConfig(max_bad_records = 1000)
        jobConfig.write_disposition = 'WRITE_TRUNCATE' 
        jobConfig.source_format = bigquery.SourceFormat.CSV
        jobConfig.skip_leading_rows = 1
        df = pd.read_csv(path)
        
    if len(df.columns) == 0:
        print("File is empty")
    else:
        sourceFile = open(path, "rb")
        
        job = client.load_table_from_file(
            sourceFile,
            tableRef,
            location="us",  # Must match the destination dataset location.
            job_config=jobConfig,
            )  # API request

        job.result()  # Waits for table load to complete.
        
        errors = job.errors
        
        sourceFile.close()
        if(fileFormat == 'json'):
            errorFile = "json_errors.txt"
        else:
            errorFile = "csv_errors.txt"
        
        nullRecords = df[df.isnull().any(axis=1) | df.eq('').any(axis=1)]
        numOfNullRecords = len(df.index)
        nullIndex = 0
        
        for error_index, error in enumerate(errors):
            error_value = "Default"
            if(fileFormat == 'csv'):
                if(re.findall("(?<=Could not parse) '\D*'", error['message'])):
                    error_value = re.findall("(?<=Could not parse) '\D*'", error['message'])[0]
                    error_value = error_value.replace('"', "")
                    error_value = error_value.replace("'", "")
                    error_value = error_value.replace(' ', "")
                    
                    found_error_record = df[df.isin([error_value]).any(axis=1)]
                    print(found_error_record)
                    with open(errorFile, "a") as f:
                        log_string = """
                        Error Message - {}
                        Inputted Data - {}
                        """.format(error['message'], found_error_record)
                        f.write(log_string)
                elif(re.findall("'[0-9.]*'", error['message'])):
                    error_value = re.findall("'[0-9.]*'", error['message'])[0]
                    error_value = error_value.replace('"', "")
                    error_value = error_value.replace("'", "")
                    error_value = error_value.replace(' ', "")
                    found_error_record = df[df.isin([error_value]).any(axis=1)]
            
                    with open(errorFile, "a") as f:
                        log_string = """
                        Error Message - {}
                        Inputted Data - {}
                        """.format(error['message'], found_error_record)
                        f.write(log_string)
                elif(re.findall("Required column value", error['message'])):
                    nullRecord = nullRecords.iloc[nullIndex]
                    nullIndex = nullIndex + 1
                    with open(errorFile, "a") as f:
                        log_string = """
                        Error Message - {}
                        Inputted Data - {}
                        """.format(error['message'], nullRecord)
                        f.write(log_string)
                else:
                    pass
            else:
                if(re.findall('"\D*"', error['message'])):
                    error_value = re.findall('"\D*"', error['message'])[0]
                    error_value = error_value.replace('"', "")
                    error_value = error_value.replace("'", "")
                    error_value = error_value.replace(' ', "")
                    found_error_record = df[df.isin([error_value]).any(axis=1)].values
            
                    with open(errorFile, "a") as f:
                        log_string = """
                        Error Message - {}
                        Inputted Data - {}
                        """.format(error['message'], found_error_record)
                        f.write(log_string)
                        
                elif(re.findall('"[0-9.]*"', error['message'])):
                    error_value = re.findall('"[0-9.]*"', error['message'])[0]
                    error_value = error_value.replace('"', "")
                    error_value = error_value.replace("'", "")
                    error_value = error_value.replace(' ', "")
                    error_value = float(error_value)
                    
                    found_error_record = df[df.isin([error_value]).any(axis=1)].values
            
                    with open(errorFile, "a") as f:
                        log_string = """
                        Error Message - {}
                        Inputted Data - {}
                        """.format(error['message'], found_error_record)
                        f.write(log_string)
                        
                elif(re.findall("Missing required field:", error['message'])):
                    nullRecord = nullRecords.iloc[nullIndex].values
                    nullIndex = nullIndex + 1
                    with open(errorFile, "a") as f:
                        log_string = """
                        Error Message - {}
                        Inputted Data - {}
                        """.format(error['message'], nullRecord)
                        f.write(log_string)
                        
                else:
                    pass
    
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucketPath)
        
        blobName = missingFileName
        blob = bucket.blob(blobName)
        
        with open(errorFile, 'rb') as file:
            blob.upload_from_file(file)
    
    
@app.route('/')
def form():
    return render_template('Flask BigQuery.html')
 
@app.route('/retry', methods = ['POST'])
def random():
    
    username = request.form['Username']
    password = request.form['Password']
    
    if len(username) <= 0:
        return render_template('Flask BigQuery Error.html', UserText = " " ,Error = "Empty Username")  
    elif len(password) <= 0:
        return render_template('Flask BigQuery Error.html', UserText = " ",Error = "Empty Password")
    else:
        if len(password) < 8:
            return render_template('Flask BigQuery Error.html', UserText = " ",Error = "Password less than 8 characters")
        else:
            query1 = "SELECT * FROM `lucid-timing-343502.User.ListOfUsers` WHERE Username = '" + username + "' and Password = '" + password + "'"

            query_res = client.query(query1)  # Make an API request.
        
            rows = query_res.result()
            dfRows = rows.to_dataframe()  # Waits for query to finish
    
            if dfRows.empty:
                query2 = "SELECT * FROM `lucid-timing-343502.User.ListOfUsers` where Username ='" + username +"'"
                query_res2 = client.query(query2)  # Make an API request.
            
                rows2 = query_res2.result()  # Waits for query to finish
                dfRows2 = rows2.to_dataframe()
            
                if dfRows2.empty:
                     return render_template('Flask BigQuery Error.html', Error = "Incorrect Username and Password")
                else:
                    return render_template('Flask BigQuery Error.html', UserText = username ,Error = "Incorrect Password")
            else:
                return redirect(url_for('upload'))
        
@app.route('/upload', methods = ['GET','POST'])
def upload():
    myFiles = getFiles(app.config['UPLOAD_FOLDER'])
    
    if request.method == 'POST':
        if request.form["submitButton"] == "File Upload":
            file = request.files['filer']
            if file and checkFile(file.filename):
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                myFiles = getFiles(app.config['UPLOAD_FOLDER'])
                return render_template('Flask BigQuery Upload.html', someFiles=myFiles)
            
        elif request.form["submitButton"] == "BQ Upload":
            selectedFiles = request.form.getlist('checks')
         
            for fileIter in selectedFiles:
                if(fileIter.rsplit('.', 1)[1].lower() == 'json'):
                    insertBQGCS('json', fileIter)
                else:
                    insertBQGCS('csv', fileIter)
                    
            return render_template('Flask BigQuery Upload.html', someFiles=myFiles)
        else:
            pass
    else:
        return render_template('Flask BigQuery Upload.html', someFiles=myFiles)

if __name__ == '__main__':
   
    app.secret_key = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'

    app.run(host = 'localhost', port = 5000 ,debug = True, use_reloader = False )
        
        
        


# In[ ]:





# In[ ]:




