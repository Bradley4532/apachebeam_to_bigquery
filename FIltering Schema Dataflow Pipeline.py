#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install apache-beam[gcp]


# In[ ]:


pip install apache-beam[interactive]


# In[ ]:


pip install pandas


# In[13]:


import apache_beam as beam
import pandas
import argparse
import re
import os
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

class FilterRecord(beam.DoFn):
    def process(self, element, table_sch):
        import apache_beam as beam
        flag = 0
        for x in range(0, len(element)):
            #print(table_sch[x]['type'])
            if(table_sch[x]['type'] == 'STRING'):
                if(element[x].strip().isalpha() == False):
                    flag = 1
                elif(len(element[x]) == 0):
                    flag = 1
                else:
                    pass
            elif(table_sch[x]['type'] == 'INTEGER'):
                if(element[x].strip().isalpha()):
                    flag = 1
                elif('.' in element[x]):
                    flag = 1
                elif(len(element[x]) == 0):
                    flag = 1
                else:
                    pass
            elif(table_sch[x]['type'] == 'FLOAT'):
                if(element[x].strip().isalpha()):
                    flag = 1
                elif('.' in element[x] == False):
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
table_ref = 'lucid-timing-343502.TestDataSet.Test'
table_sch = []

client = bigquery.Client()

try:
    SchemaJob = client.get_table(table_ref)
    #print("Table Schema: {}".format(SchemaJob.schema))
    for s in SchemaJob.schema:
        new_dict = {}
        new_dict['name'] = s.name
        new_dict['type'] = s.field_type
        new_dict['mode'] = s.mode
        table_sch.append(new_dict)
    
    print(table_sch)
    #OUTPUT: [{'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'id', 'type': 'FLOAT', 'mode': 'NULLABLE'}]
except Exception as e:
    pass
    

with beam.Pipeline() as pipeline:
    good, bad = (
        pipeline
        | beam.io.ReadFromText("CSVFileTest.csv", skip_header_lines = True)
        | beam.Map(lambda x : x.split(","))
        | beam.ParDo(FilterRecord(), table_sch).with_outputs("Good", "Bad")
   )
    #good| 'Good print' >> beam.Map(print)
    bad | 'Bad print' >> beam.Map(print)


