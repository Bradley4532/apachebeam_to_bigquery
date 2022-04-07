#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install apache-beam[gcp]


# In[ ]:


pip install apache-beam[interactive]


# In[ ]:


pip install pandas


# In[53]:


import apache_beam as beam
import pandas
import argparse
from apache_beam.options.pipeline_options import PipelineOptions

class FilterRecord(beam.DoFn):
    def process(self, element):
        flag = 0
        for x in range(0, len(element)):
            if(element[x].strip().isalnum()):
                flag = 1
            elif(element[x].strip):
            else:
                flag = 1
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


# In[ ]:


import apache_beam as beam
import pandas
import argparse
import os
from apache_beam.options.pipeline_options import PipelineOptions

class FilterRecord(beam.DoFn):
    def process(self, element):
        flag = 0
        for x in range(0, len(element)):
            if(element[x].strip()):
                pass
            else:
                flag = 1
        if(flag == 0):
            yield beam.pvalue.TaggedOutput('Good', element)
        else:
            yield beam.pvalue.TaggedOutput('Bad', element)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'lucid-timing-343502-bcf3aa48c017.json'
input_file = 'gs://dataflow-samples/shakespeare/kinglear.txt'
output_path = 'gs://random-bucket-124125412/counts.txt'

parser = argparse.ArgumentParser()

parser.add_argument(
    '--input-file',
    default='gs://dataflow-samples/shakespeare/kinglear.txt',
    help='The file path for the input text to process.'
)
parser.add_argument(
    '--output-path', default = 'gs://random-bucket-124125412/counts.txt', help='The path prefix for output files.')

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
        | beam.io.ReadFromText(args.input_file)
        | beam.Map(lambda x : x.split(","))
        | beam.ParDo(FilterRecord()).with_outputs("Good", "Bad")
        | beam.io.WriteToText(args.output_path)
    )
    #s = (good
    #     | 'Good print' >> beam.io.WriteToText(args.output_path, shard_name_template = "")
    #)
    #d = ( bad
    #     | 'Bad print'>> beam.io.WriteToText(args.output_path, , shard_name_template = "")
    #     )


# In[ ]:




