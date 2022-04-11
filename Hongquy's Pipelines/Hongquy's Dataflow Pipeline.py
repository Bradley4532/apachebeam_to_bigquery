#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install apache-beam[gcp]


# In[ ]:


pip install apache-beam[interactive]


# In[ ]:


pip install pandas


# In[16]:


#import apache_beam as beam
#import pandas
#import argparse
#import re
#from apache_beam.options.pipeline_options import PipelineOptions

#class FilterRecord(beam.DoFn):
#    def process(self, element):
#        flag = 0
#        for x in range(0, len(element)):
#            if(element[x].strip().isalpha()):
#                flag = 1
#            elif('.' in element[x]):
#                flag = 1
#            elif(len(element[x]) == 0):
#                flag = 1
#            else:
#                pass
#        if(flag == 0):
#            yield beam.pvalue.TaggedOutput('Good', element)
#        else:
#            yield beam.pvalue.TaggedOutput('Bad', element)
    
#with beam.Pipeline() as pipeline:
#    good, bad = (
#        pipeline
#        | beam.io.ReadFromText("CSVFileTest.csv", skip_header_lines = True)
#        | beam.Map(lambda x : x.split(","))
#        | beam.ParDo(FilterRecord()).with_outputs("Good", "Bad")
#   )
 #   good| 'Good print' >> beam.Map(print)
   


# In[17]:


import apache_beam as beam
import pandas
import argparse
import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


class FilterRecord(beam.DoFn):
    def process(self, element):
        import apache_beam as beam
        flag = 0
        for x in range(0, len(element)):
            if(element[x].strip().isalpha()):
                flag = 1
            elif('.' in element[x]):
                flag = 1
            elif(len(element[x]) == 0):
                flag = 1
            else:
                pass
        if(flag == 0):
            yield beam.pvalue.TaggedOutput('Good', element)
        else:
            yield beam.pvalue.TaggedOutput('Bad', element)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'lucid-timing-343502-bcf3aa48c017.json'
input_file = 'gs://random-bucket-124125412/CSVFileTest.csv'
output_path = 'gs://random-bucket-124125412/counts.txt'

table_spec = bigquery.TableReference(
    projectId='lucid-timing-343502',
    datasetId='TestDataSet',
    tableId='Test')

table_schema = {
    'fields': [
        {'name': 'key', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'value', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'test', 'type': 'STRING', 'mode': 'NULLABLE'}
    ]
}

parser = argparse.ArgumentParser()

parser.add_argument(
    '--input-file',
    default='gs://random-bucket-124125412/CSVFileTest.csv',
    help='The file path for the input text to process.'
)

parser.add_argument(
    '--output-path', 
    default = 'gs://random-bucket-124125412/',
    help='The path prefix for output files.')

parser.add_argument(
        '--save_main_session',
        default=True,
        help = 'This helps with namespace stuff')

args, beam_args = parser.parse_known_args()

beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project='lucid-timing-343502',
    job_name='randomjob21415',
    temp_location='gs://random-bucket-124125412/temp',
    region='us-central1'
)

with beam.Pipeline(options=beam_options) as pipeline:
    good, bad = (
        pipeline
        | beam.io.ReadFromText('gs://random-bucket-124125412/CSVFileTest.csv', skip_header_lines = True)
        | beam.Map(lambda x : x.split(","))
        | beam.ParDo(FilterRecord()).with_outputs("Good", "Bad")
    )
    #good | "print" >> beam.Map(print)
  # good | beam.io.WriteToBigQuery(
  #      table_spec,
  #      schema=table_schema,
  #      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
  #      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED) 
    
    bad | beam.io.WriteToText('gs://random-bucket-124125412/Bad.csv', shard_name_template = "")
  


# In[ ]:




