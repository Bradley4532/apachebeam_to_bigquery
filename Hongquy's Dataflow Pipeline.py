#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install apache-beam[gcp]


# In[ ]:


pip install apache-beam[interactive]


# In[ ]:


pip install pandas


# In[ ]:


import apache_beam as beam
import pandas
import argparse
import re
from apache_beam.options.pipeline_options import PipelineOptions

class FilterRecord(beam.DoFn):
    def process(self, element):
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
    
with beam.Pipeline() as pipeline:
    good, bad = (
        pipeline
        | beam.io.ReadFromText("CSVFileTest.csv", skip_header_lines = True)
        | beam.Map(lambda x : x.split(","))
        | beam.ParDo(FilterRecord()).with_outputs("Good", "Bad")
    )
    s = (good
         | 'Good print' >> beam.io.WriteToText("GoodTest.csv", shard_name_template = "")
    )
    d = ( bad
         | 'Bad print'>> beam.io.WriteToText("BadTest.csv", shard_name_template = "")
         )


# In[5]:


import apache_beam as beam
import pandas
import argparse
import os
from apache_beam.options.pipeline_options import PipelineOptions

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
    s = (good
         #| 'Good print' >> beam.io.WriteToText(args.output_path, shard_name_template = "")
         | 'Good print' >> beam.io.WriteToText('gs://random-bucket-124125412/Good.csv', shard_name_template = "")
    )
    d = ( bad
         #| 'Bad print'>> beam.io.WriteToText(args.output_path, , shard_name_template = "")
         | 'Bad print' >> beam.io.WriteToText('gs://random-bucket-124125412/Bad.csv', shard_name_template = "")
         )


# In[ ]:




