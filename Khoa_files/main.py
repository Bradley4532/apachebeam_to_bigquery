import apache_beam as beam
import pandas
import re
import argparse
import os
import sys
import time
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='flask-upload-345423',
    temp_location='gs://sample_flask_ktd/temp',
    region='us-central1'
)

#A little too "hardcodey". Can Get schema from table in BigQuery TBD
project_id = 'flask-upload-345423'
dataset_id = 'sample'
table_id = 'email'

table_schema = 'name:STRING,email:STRING,id:INTEGER'

def run():
    start = time.time()
    pipe = beam.Pipeline(options=beam_options)
    ip = (pipe
        | 'ReadData' >> beam.io.ReadFromText("email.csv", skip_header_lines=True)
        | 'SplitData' >> beam.Map(lambda x: x.split(','))
        | 'FormatToDict' >> beam.Map(lambda x: {"name": str(x[0]), "email": str(x[1]), "id": int(x[2])})
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=table_id,
            dataset=dataset_id,
            project=project_id,
            schema = table_schema,
            method='STREAMING_INSERTS',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED        
        )
    )

    result = pipe.run().wait_until_finish()
    end = time.time()
    print("It takes", end - start, "seconds to execute.")


if __name__ == '__main__':
    
    path_service_account = 'flask-upload-345423-cbb52ca10b67.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account
    run()
    
    
    