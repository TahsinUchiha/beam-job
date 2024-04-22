import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from transforms.transaction_processing import TransactionProcessing
import requests
import os

# GCS File
file='https://storage.googleapis.com/cloud-samples-data/bigquery/sample-transactions/transactions.csv' 

# Download the file from GCS
response = requests.get(file)

# Check if the request was successful
if response.status_code == 200:
    print('Successfully retrieved file')
    lines = response.text.split('\n')
else:
    print(f"Failed to download file: {response.status_code}, {response.text}")
    lines = []


def run(argv=None):
    # Set up the pipeline options
    options = PipelineOptions()

    with beam.Pipeline(options = options) as pipeline:
        transactions = (
            pipeline 
            # Directly read from gcs but this requires authentication 
            # | 'Read CSV' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
            | 'Read CSV' >> beam.Create(lines)
            | TransactionProcessing()
            | 'Write JSONL' >> beam.io.WriteToText('output/results.jsonl.gz', header='date,total-amount', compression_type=beam.io.textio.CompressionTypes.GZIP,)
        )
    os.rename('output/results.jsonl.gz-00000-of-00001', 'output/results.jsonl.gz')
if __name__ == '__main__':
    run()

    