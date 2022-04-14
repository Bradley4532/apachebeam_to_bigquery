import apache_beam as beam
import pandas
import argparse
import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
import google.cloud.logging

class formating(beam.DoFn):
    def process(self, element, table_sch):
        import apache_beam as beam
        record = {}
        for x in range(0, len(element)):
            name = table_sch[x]['name']
            record[name] = element[x]
        yield record
        
def list_sinks():
    """Lists all sinks."""
    logging_client = logging.Client()

    sinks = list(logging_client.list_sinks())

    if not sinks:
        print("No sinks.")

    for sink in sinks:
        print("{}: {} -> {}".format(sink.name, sink.filter_, sink.destination))  
        
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'lucid-timing-343502-bcf3aa48c017.json'
client = google.cloud.logging.Client(project="lucid-timing-343502")

SCHEMA = {
    'fields': [
        {'name': 'key', 'type': 'STRING', 'mode': 'NULLABLE'}, 
        {'name': 'value', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'test', 'type': 'STRING', 'mode': 'NULLABLE'}
    ]
}

table_sch = [{'name': 'key', 'type': 'STRING', 'mode': 'NULLABLE'},
             {'name': 'value', 'type': 'STRING', 'mode': 'REQUIRED'},
             {'name': 'test', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]
beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='lucid-timing-343502',
    job_name='randomjob21415',
    temp_location='gs://random-bucket-124125412/temp',
    region='us-central1'
)

#p = beam.Pipeline(options=beam_options)

#events  = (p | 'ReadData' >> beam.io.ReadFromText('gs://random-bucket-124125412/CSVFileTest.csv', skip_header_lines =1)
  #           | 'Split' >> beam.Map(lambda x: x.split(','))
  #           | 'format to dict2' >> beam.ParDo(formating(),table_sch) 
  #           | 'WriteToBigQuery2' >>  beam.io.gcp.bigquery.WriteToBigQuery(
 #                                         'lucid-timing-343502:TestDataSet.Test',
 #                                         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
 #                                         insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_NEVER,
#                                          method='STREAMING_INSERTS')
#            )
  
#(events[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]
# | "Bad lines" >> beam.io.WriteToText('gs://practice_error_logs/{}'.format("practiceError"), shard_name_template = ""))

#result = p.run()
#list_sinks()
#filter_str = (
#    f'logName="projects/lucid-timing-343502/logs/dataflow.googleapis.com%2Fworker"'
#    f' AND resource.type="dataflow_step"'
#    f' AND severity >= ERROR'
#)
filter_str = (
    f' resource.type="dataflow_step"'
    f' AND severity >= ERROR'
)
client = google.cloud.logging.Client()
logger = client.logger(name="dataflow.googleapis.com%2Fworker")
#for entry in client.list_entries(filter_=filter_str):
#    print(entry)
for entry in logger.list_entries(filter_=filter_str):  # API call(s)
    print(entry)
