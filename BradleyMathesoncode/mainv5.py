#Project: Import json to bigquery table
#StartDate: 4/06/2022
#EndDate: 
#Developer: Bradley Matheson

#Need to allow multiple files to be selected---------------


from flask import Flask,render_template,request
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import datetime, re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from werkzeug.utils import secure_filename



#this is needed to run the website html
app = Flask(__name__)

#secret key is to allow this python code to move variable from the frontend to the backend
app.secret_key = "35367565"

#this is to allow a authenticated user on the project to work on bigquerry
Key_path = "C:\\Users\Brad\Documents\model-craft-342921-ea36cdb339e7.json"

#this is needed to request a job
credentials = service_account.Credentials.from_service_account_file(Key_path,scopes=["https://www.googleapis.com/auth/cloud-platform"])
client=bigquery.Client(credentials=credentials,project=credentials.project_id)
table_id='model-craft-342921.testing.Task2'
storage_client=storage.Client(credentials=credentials,project=credentials.project_id)
bucket = storage_client.get_bucket("data_intake4062022")

beam_options = PipelineOptions(
                            runner = 'DataflowRunner',
                            project = 'model-craft-342921',
                            job_name = 'pythontobigquerry',
                            temp_location = 'gs://dataflow_example41622/temp1',
                            service_account_email = 'practice-py@model-craft-342921.iam.gserviceaccount.com',
                            GOOGLE_APPLICATION_CREDENTIALS = Key_path
                        )
#input_path = 'gs://dataflow_example41622/data_intake'


def get_error_report(errors, file_dataframe):
    report = []
    isNull = False
    nullIndex = 0
    column_Names = ["name","id","salary_in_k","phonenumber"]
    try:
        for error_index, error in enumerate(errors):
            error_location = re.findall(r'\d+', error['message'])
            message = error['message']
            try:
                error_value = re.findall(r"'(.*?)'", error['message'])[0]
                found_error_records = file_dataframe[file_dataframe.isin([error_value]).any(axis=1)].values
                error_message = message.replace("field id (position {}) starting at location {}".format(error_location[-2],error_location[-1]),"""column '{}'""".format(column_Names[int(error_location[-2])]))
            except:
                #found_error_records = 'null'
                # Get all the rows containing null values in required columns and get their row numbers
                found_error_records = file_dataframe[file_dataframe.isna().any(axis=1)].values
                error_message = message.replace("index: {} is missing in row starting at position: {}".format(error_location[-2],error_location[-1]),"""'{}'""".format(column_Names[int(error_location[-2])]))
                isNull = True
            if isNull:
                found_error_records = found_error_records[nullIndex]
                nullIndex+=1
                isNull = False
            log_string = """| Error # - {} || Error Message - {} || Inputted Data - {} |""".format(error_index, error_message, found_error_records)
            report.append(log_string)
    except:
        raise
    #return str(report)
    return report


#This will render the frontend of the website to get the json file
@app.route('/', methods=['GET','POST'])
def index():

    #resets the message so that previous messages dont confuse the user
    message = None

    empty = False

    #This will only run if the user attempts to submit a file
    if request.method == 'POST':

        #this will only run if what the user submitted is a file
        if 'file' in request.files:

            #this gets the file data
            file = request.files['file']
            #this aquires the name of the file
            filename = secure_filename(file.filename)
            if filename.endswith('.csv'):

                #this will only run if the file is a csv
                if filename.endswith('.csv'):
                    blob = bucket.blob('data.txt')
                    blob.upload_from_string(str(file.read()))
                    try:
                        with beam.Pipeline(options = beam_options) as pipeline:
                            lines = pipeline | 'ReadMyFile' >> beam.io.ReadFromText('gs://data_intake4062022/data.txt')

                        print(lines)
                    except Exception as error:
                        print('This was the error: ', error)
                        message = "Error setting up the pipeline"
                    
    
            #If the file is not a json or the csv this will run
            else:
                message = "File type is not excepted"
            #endif

        #This will run if the submition is not a file type
        elif 'file' not in request.files:
            message = "There was no file to upload"
        #endif
    #endif

    #this will render the template on the website
    return render_template("front.html", message = message)

if __name__ == '__main__':
    app.run()