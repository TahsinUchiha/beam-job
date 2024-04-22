import apache_beam as beam
import csv 
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
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
        # Read the input CSV file
        transactions = (
            pipeline
            # | 'Read CSV' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
            | 'Read CSV' >> beam.Create(lines)
            | beam.Map(lambda line: next(csv.reader([line])))
            | beam.Map(lambda row: tuple(row))  # Convert list to tuple 
            | 'Remove duplicates' >> beam.Distinct()  # Remove duplicates based on the key
            | beam.Map(lambda row: list(row))  # Convert the tuple back to a list
            | beam.Filter(lambda row: row[0] != 'timestamp')
        )

        # Filter transactions with amount > 20 amd exclude transactions before year - 2010

        filtered_transactions = (
            transactions
            | beam.Filter(lambda row: float(row[3]) > 20) # transaction_amount > 20
            | beam.Filter(lambda row: int(row[0].split('-')[0]) >= 2010) # Exclude transactions before 2010
        )

        # Calculate the total transaction amount grouped by date

        totals = (
            filtered_transactions
            | beam.Map(lambda row: (datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S %Z').strftime('%Y-%m-%d'), float(row[3]))) # (date, transaction_amount)
            | beam.CombinePerKey(sum) # Sum the transaction amounts by timestamp
        )

        # Write the output as a JSONL file
        totals | 'Write JSONL' >>  beam.io.WriteToText('output/results.jsonl.gz', file_name_suffix='.gz', header='date,total-amount', compression_type=beam.io.textio.CompressionTypes.GZIP, num_shards=1,)
        # totals | 'Write JSONL' >> beam.io.WriteToText('output/results.jsonl', file_name_suffix='.jsonl', header='date,total-amount')
        os.rename('output/results.jsonl.gz-00000-of-00001', 'output/results.jsonl.gz')
if __name__ == '__main__':
    run()

    