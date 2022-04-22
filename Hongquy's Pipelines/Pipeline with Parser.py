#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Project: Import csv to bigquery table
#StartDate: 4/06/2022
#EndDate: 
#Developer: Bradley, Hongquy, Khoa
from flask import Flask,render_template,request,redirect,url_for
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import os, re, datetime, uuid, time, json
import google.cloud.logging
import apache_beam as beam
from io import StringIO
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from werkzeug.utils import secure_filename
#this is to allow a authenticated user on the project to work on bigquerry
Key_path = r"lucid-timing-343502-bcf3aa48c017.json"

#this is needed to request a job
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Key_path
credentials = service_account.Credentials.from_service_account_file(Key_path,scopes=["https://www.googleapis.com/auth/cloud-platform"])
client=bigquery.Client(credentials=credentials,project=credentials.project_id)

#this is needed to allow the request to access gcp cloud storage
storage_client=storage.Client(credentials=credentials,project=credentials.project_id)

#this is where a temporary file is stored to be accessed by the cloud
bucket = storage_client.get_bucket("temp_bucket_1999")

#this is where the column location and message from bigquery is stored to be accessed later
bucket1 = storage_client.get_bucket("error_message_bucket_1999")

#this is where the rows attempted to be inserted into bigquery is stored to be accessed later
bucket2 = storage_client.get_bucket("error_rows_bucket_1999")

bucket3 = storage_client.get_bucket("job_bucket_1999")

#this is needed to access logs from the bigquery job
client1 = google.cloud.logging.Client()
logger = client1.logger(name="dataflow.googleapis.com%2Fworker")

parser = argparse.ArgumentParser()

parser.add_argument(
    '--num_workers',
    default=10,
    help='Increasing number of workers.')
    
args, beam_args = parser.parse_known_args()

print(beam_args)    
#this is a function created to format the input data into a dictionary so the data can be easily inserted into bigquery
class formating(beam.DoFn):
    #table_sch is a side input that was found for the table we are trying to insert the data into
    def process(self, element, table_sch, job_name):
        import apache_beam as beam
        record = {}
        if len(element) <= len(table_sch)-1:
            record['job_id'] = job_name
            for x in range(0, len(element)):
                name = table_sch[x]['name']
                if element[x] == '':
                    record[name] = None
                else:
                    record[name] = element[x]
            yield beam.pvalue.TaggedOutput('Good', record)
        else:
            yield beam.pvalue.TaggedOutput('Bad', element)
            yield beam.pvalue.TaggedOutput('Bad1', element)
class formating2(beam.DoFn):
    def process(self, element, table_id):
        import apache_beam as beam
        element = element[1]
        element.pop('job_id', None)
        yield element
class mess(beam.DoFn):
    def process(self, element):
        import apache_beam as beam
        string = 'Error #Before Insert - Message: Too many columns - Input: {}\n'.format(",".join(element))
        yield string
#this function is for the second pipeline and will merge the message from bigquery with its respective input data
class process_message(beam.DoFn):
    #bad_input_data is a side input that contains all the bad inserts from bigquery
    #Total is a side input that will be used for numbering the error message
    def process(self, element, bad_input_data, count):
        import apache_beam as beam
        from io import StringIO
        import json, re
        #these few steps are required to format the data
        bad_message = json.loads(element)
        #the last steps take an element from the pipeline, which is the message and column location, and match it with the side input
        for bad_message_row in bad_message['elements']:
            for input_data in bad_input_data['elements']:
                message = ''
                if bad_message_row['message'].find('cannot be empty.') == -1:
                    if input_data[bad_message_row['location']] == bad_message_row['message'].split(': ')[1]:
                        count = count+1
                        #this is the final message that will be seen by the user
                        message = 'Error #{} - Message: {} in column {} - Input: {}\n'.format(count, bad_message_row['message'],bad_message_row['location'],",".join(input_data.values()))
                        #this will remove the used inputed row data so it wont be used more than once
                        bad_input_data['elements'].remove(input_data)
                        yield beam.pvalue.TaggedOutput('message', message)
                        yield beam.pvalue.TaggedOutput('data', ",".join(input_data.values()))
                else:
                    if input_data[bad_message_row['location']] == '':
                        count = count+1
                        #this is the final message that will be seen by the user
                        message = 'Error #{} - Message: {} in column {} - Input: {}\n'.format(count, bad_message_row['message'],bad_message_row['location'],",".join(input_data.values()))
                        #this will remove the used inputed row data so it wont be used more than once
                        bad_input_data['elements'].remove(input_data)
                        yield beam.pvalue.TaggedOutput('message', message)
                        yield beam.pvalue.TaggedOutput('data', ",".join(input_data.values()))
                    
#this is needed to run the website html
app = Flask(__name__)

#secret key is to allow this python code to move variable from the frontend to the backend
app.secret_key = "35367565"

@app.route('/')
def form():
    return redirect(url_for('upload'))

#This will render the frontend of the website to get the json file
@app.route('/upload', methods=['GET','POST'])
def upload():

    #resets the message so that previous messages dont confuse the user
    message = None
    
    #this will find all the tables being used in this project and display them as options to be used in bigquery
    tables = ['Test'] #change--------------------------
    
    #this creates a unique job name for dataflow
    job_name = 'pythontobigquerry-{}'.format(uuid.uuid4())
    
    beam_options = PipelineOptions(
                                beam_args,
                                runner = 'DataflowRunner',
                                #runner='DirectRunner',
                                project = 'lucid-timing-343502',
                                job_name = '{}'.format(job_name),
                                temp_location = 'gs://temp_bucket_1999/temp1',
                                region='us-central1',
                    
                    )
    
    #this is needed to get the error logs from the bigquery in dataflow job
    filter_str = (
        f'resource.labels.job_name={job_name}'
        f' resource.type="dataflow_step"'
        f' AND severity >= ERROR'
    )
    
    
    #This will only run if the user attempts to submit a file
    if request.method == 'POST':

        #this will only run if what the user submitted is a file
        if 'file' in request.files:
            #this gets the file data
            file = request.files['file']
            #this aquires the name of the file
            filename = secure_filename(file.filename)
            
            #this will check it the file has data to be processed
            if len(file.readlines()) == 0:
                message = "File has no data to process"
            else:
                file.seek(0)
                try:
                    #this will only run if the file is a csv
                    if filename.endswith('.csv'):
                        
                        #these two lines are to upload the data fetched from the front-end to gcp cloud storage
                        blob = bucket.blob('data.csv')
                        blob.upload_from_file(file, timeout=3600)
                        
                        file.seek(0)
                        #this gets the total number of records to be inserted to bigquery. its a side input
                        total_records = len(file.readlines())
                        
                        
                        #this gets the table wanting to be used from the front end
                        table_id = request.form.getlist('checks')[0]
                        print(table_id)
                        SCHEMA = {}
                        table_sch = []
                        #in this try block it is attempting to get the table schema. its a side input
                        try:
                            SchemaJob = client.get_table('lucid-timing-343502.TestDataSet.{}'.format(table_id))
                            for s in SchemaJob.schema:
                                new_dict = {}
                                new_dict['name'] = s.name
                                new_dict['type'] = s.field_type
                                new_dict['mode'] = s.mode
                                table_sch.append(new_dict)
                            SCHEMA['fields'] = table_sch

                        except Exception as e:
                            print(e)
                        #these two lines create a unique name for the files being saved to gcp cloud storage. files with the same name but in differnt buckets are tied together
                        filename_helper = str(datetime.datetime.now())
                        filename = "LOG: "+ filename_helper
                        
                        #in this try block, it will attempted to create the first pipeline that will read the input data and try to write it to bigquery. all failed rows with be stored into the gcp cloud storage
                        try:
                            start = time.time()
                            p = beam.Pipeline(options=beam_options)
                            good, bad, bad1  = (p | 'ReadData' >> beam.io.ReadFromText('gs://temp_bucket_1999/data.csv', skip_header_lines =1)
                                   | 'Split' >> beam.Map(lambda x: x.split(','))
                                    | 'format to dict2' >> beam.ParDo(formating(),table_sch, job_name).with_outputs("Good", "Bad", "Bad1")
                                    )
                            (bad | 'csv format' >> beam.Map(lambda x: ",".join(x))
                                 | "too many columns" >> beam.io.WriteToText('gs://complete_message_bucket_1999/{}_bad_rows.csv'.format(filename), shard_name_template = "")
                            )
                            (bad1 | "Assign message" >> beam.ParDo(mess())
                                    | 'Store Message' >> beam.io.WriteToText('gs://complete_message_bucket_1999/{}_message.txt'.format(filename), shard_name_template = "")
                            )
                            events  = (good | 'WriteToBigQuery2' >>  beam.io.gcp.bigquery.WriteToBigQuery(
                                       'lucid-timing-343502:TestDataSet.{}'.format(table_id),
                                       schema=SCHEMA,
                                       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                       batch_size = 200000,
                                       insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_NEVER,
                                       method='STREAMING_INSERTS')
                                    )
                            (events[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]
                                     | "remove tuple" >> beam.ParDo(formating2(),table_id)
                                    | "Bad lines" >> beam.io.WriteToText('gs://error_rows_bucket_1999/{}.txt'.format(filename), append_trailing_newlines=True, shard_name_template = "")
                            )
                            
                            
                            result = p.run()
                            result.wait_until_finish()
                        except Exception as error:
                            print('This was the error: ', error)
                            message = "Error setting up the data ingestion pipeline"
                        
                        #this try block will attempted to fetch the logs needed to complete the erroring messaging system for bad row inserts
                        try:
                            log = {}
                            log_builder = {}
                            log_array = []
                            for entry in logger.list_entries(filter_=filter_str):  # API call(s)
                                #these few lines will extract the needed data and get rid of everything that isnt column location and bigquery response message
                                message = entry.to_api_repr()['jsonPayload']['message']
                                list_extract = message.replace("There were errors inserting to BigQuery. Will not retry. Errors were ","")
                                list_extract = list_extract.replace("'",'"')
                                res = json.loads(list_extract)
                                #these few lines will save the needed data into a dictionary
                                for i in range(0,len(res)):
                                    log_builder = {}
                                    log_builder['location'] = res[i]['errors'][0]['location']
                                    log_builder['message'] = res[i]['errors'][0]['message']
                                    log_array.append(log_builder)
                            log['elements'] = log_array
                            with open('temp.txt', 'a') as outfile:
                                outfile.write(json.dumps(log)+'\n')
                            #these few lines are uploading the temp file to gcp cloud storage
                            blob = bucket1.blob('{}.txt'.format(filename))
                            blob.upload_from_filename(r"C:\Users\Hongquy\Documents\Bradely's Pipeline\temp.txt", timeout=3600)
                            total_bad_records = len(log_array)
                            os.remove("temp.txt")
                            message = "Data uploaded to the Bigquery"
                            
                        except Exception as error:
                            print('This was the error: ', error)
                            message = "Error getting logs"
                        
                        #this try block is creating a side input with all the bad rows attempted to be inserted into bigquery
                        try:
                            blob = bucket2.blob('{}.txt'.format(filename))
                            downloaded_blob = blob.download_as_string()
                            log_message = downloaded_blob.decode("utf-8", "ignore")
                            log_message = log_message.replace("'",'"')
                            log_message = log_message.replace('None', '""')
                            bad_list =log_message.split('\n')
                            bad_json = []
                            bad_input_data = {}
                            for row in bad_list:
                                if row != '':
                                    j = json.loads(row)
                                    bad_json.append(j)
                            bad_input_data['elements'] = bad_json
                        except Exception as error:
                            print('This was the error: ', error)
                            message = "Error setting up messaging"
                            
                        
                        #in this try block, this is the second pipeline that generates the final message that will be read by the user
                        try:
                            count =0
                            p1 = beam.Pipeline(options=beam_options)
                            events1, events2  = (p1 | 'ReadData' >> beam.io.ReadFromText('gs://error_message_bucket_1999/{}.txt'.format(filename))
                                           | 'Process Messaging' >> beam.ParDo(process_message(),bad_input_data,count).with_outputs("message", "data")
                            )
                            (events1 | 'Store Final Message' >> beam.io.WriteToText('gs://complete_message_bucket_1999/{}_message1.txt'.format(filename), shard_name_template = "")
                            )
                            (events2 | 'Store data' >> beam.io.WriteToText('gs://complete_message_bucket_1999/{}_bad_rows1.csv'.format(filename), shard_name_template = "")
                            )
                            
                            result = p1.run()
                            result.wait_until_finish()
                        
                        except Exception as error:
                            print('This was the error: ', error)
                            message = "Error setting up the error formating pipeline"
                        
                        end = time.time()
                        Total_time = end - start
                        
                        # Good elements - read number of elements in BigQuery table
                        query = """SELECT count(*) FROM `lucid-timing-343502.TestDataSet.{}` WHERE job_id = '{}'""".format(str(table_id),job_name)
                        try:
                            results =  client.query(query)
                        except Exception as e:
                                print("ERROR: could not query")
                        num_good = ""
                        for row in results:
                            num_good = row["f0_"]
                        #Write to GCP bucket
                        msg = '''Time elasped: {} seconds, Number of Rows: {}, Number of Valid Rows: {}, Number of Invalid Rows: {}\n'''.format(str(Total_time), str(total_records-1), str(num_good), str(total_bad_records))

                        blob = bucket3.blob("metrics")
                        blob.upload_from_string(msg, timeout=3600)
                    
                    #If the file is not a json or the csv this will run
                    else:
                        message = "File type is not excepted"
                    #endif
                except Exception as error:
                    print('This was the error: ', error)
                    message = "There was an error in creating the request"
            #endif
        #This will run if the submition is not a file type
        elif 'file' not in request.files:
            message = "There was no file to upload"
        #endif
    #endif

    #this will render the template on the website
    return render_template("front.html", message = message, tables = tables)
if __name__ == '__main__':
    app.run()


# In[ ]:




