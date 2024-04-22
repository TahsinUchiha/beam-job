# Documentation on Apache Beam Job

This documentation servers as a purpose of the steps/procedures taken during the development of `beam_pipeline.py` and `beam_pipeline_composite.py`, the entire process can also be executed from `runbeam.sh` script. Tests can be executed by running `runtests.sh`. Documentation here servers as a purpose of reproducing the steps and any learning occurred during the process. 



## Steps 
There are three ways: 
- First we can pull the file locally and process it locally. This however may still require authentication when using a gsutil command. 
To pull the file this way, since file exists in 
`gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
We will still need to configure `gcloud`, and use `gsutil` to copy it. The disadvantages here is that file could be huge and would take up significant storage space.
- Second, we can directly give the path in our pipeline and read from there. This has advantage if the file is exceptionally large, allowing us to directly process from cloud.
- Third, and what we will be doing in our case will be using the `requests` library to do a get request, download the file and process it directly. This has the most advantage as we can avoid authentication steps this way and directly process the file. 

### **To configure for first two suggestions (Otherwise skip to [Step 2](#Step-2))**

We will still need to configure `gcloud`, and use `gsutil` to copy it.
- Directly give the path in our pipeline and read from there. This has advantage if the file is exceptionally large, allowing us to directly process. 

### Step 1.1
- Configure `gcloud`
	- Install gcloud SDK - https://cloud.google.com/sdk/docs/install-sdk
	- Run `gcloud init` - initialise your gcloud account, authenticate, select project.
- Next copy file using `gsutil` to your desired directory, in my case in the current directory. 
    ```gsutil cp gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv .```

### Step 1.2 
With this step we can configure our beam to Read directly from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
- `pip install apache-beam[gcp]`
- Run the following steps before executing:
	- `gcloud auth application-default login`
	- `export GOOGLE_APPLICATION_CREDENTIALS=<credential_file>.json`


We can now start writing the pipeline.
### Step 2

- Create beam_pipeline.py
	- Import necessary packages, 
```
import apache_beam as beam
import csv 
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import requests
```

- Get the file ready, specifying the `https://https://storage.googleapis.com/` link, as this is where the location of the file will be. 
```
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

```


- Create the `run()` function which will be called and the first Beam pipeline, `transactions` which will have:
    - First line in the pipeline, 'Read CSV' reads from input, if we wanted to read directly from `gs://` this would require additional authentication. However since we are downloading the file via get request, we can simply read the CSV file that we have processed. 
        - The `beam.Create(lines)` creates a PCollection from the lines list, where each element in the PCollection is a line. 
    ```
    def run(argv=None):
    # Set up the pipeline options
    options = PipelineOptions()

    with beam.Pipeline(options = options) as pipeline:
        # Read the input CSV file
        transactions = (
            pipeline
            # | 'Read CSV' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
            | 'Read CSV' >> beam.Create(lines)
    ```
    
    - Next line uses the Map transform to apply a function to each element in the **PCollection** (the output of the previous transform), we use a lambda function that uses csv.reader to parse each line as CSV. 
    ```
            | beam.Map(lambda line: next(csv.reader([line])))
    ```
    - For the next 3 PTransforms, I am first converting each row into a tuple so that we can apply the transform `beam.Distinct()`, since this requires input to be hashable and lists are not hashable in python. The `beam.Distinct()` is applied to remove duplicate rows from the PCollection. Once applied, we turn it back into a list. 
    ```
            | beam.Map(lambda row: tuple(row))  # Convert list to tuple 
            | 'Remove duplicates' >> beam.Distinct()  # Remove duplicates based on the key
            | beam.Map(lambda row: list(row))  # Convert the tuple back to a list
    ```
    - Next we filter the header row, so only the data is processed. In this case we know the first column is `timestamp`
    ```
            | beam.Filter(lambda row: row[0] != 'timestamp')
    ```
- Next part of the pipeline transformation chain is to create our filter conditions, `filtered_transactions`, by pulling from the previous transactions pipeline
    - First part filters for the column = transaction_amount, values > 20
    - Second part filters for the timestamp >= 2010 
    ```
        # Filter transactions with amount > 20 amd exclude transactions before year - 2010

        filtered_transactions = (
            transactions
            | beam.Filter(lambda row: float(row[3]) > 20) # transaction_amount > 20
            | beam.Filter(lambda row: int(row[0].split('-')[0]) >= 2010) # Exclude transactions before 2010
        )
    ```

- Then to calculate total, we create the `totals` pipeline, starting from previous pipeline
    - The first part creates a tuple of (date, transaction_amount), stripping the timestamp into the 
    `YYYY-mm-dd` format.
    - Second part, `beam.CombinePerKey(sum)` combines the float values for each date using the `sum` function.
    ```
        # Calculate the total transaction amount grouped by date

        totals = (
            filtered_transactions
            | beam.Map(lambda row: (datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S %Z').strftime('%Y-%m-%d'), float(row[3]))) # (date, transaction_amount)
            | beam.CombinePerKey(sum) # Sum the transaction amounts by timestamp
        )
    ```
- Finally we write to a file `output/results.jsonl.gz`, giving the header, compression_type.
    ```
        # Write the output as a JSONL file
        # totals | 'Write JSONL' >>  beam.io.WriteToText('output/results.jsonl.gz', file_name_suffix='.gz', header='date,total-amount', compression_type=beam.io.textio.CompressionTypes.GZIP)
        totals | 'Write JSONL' >> beam.io.WriteToText('output/results.jsonl', file_name_suffix='.jsonl', header='date,total-amount')
    ```
- Run from command line
    `python3 beam_pipeline.py`

## Step 3

For this we combine the transformation procedures into a single composite transform class, this is done so that the processing logic is reusable, making code cleaner and easier to understand. 
Because Composite transforms can be nested, it allows us to build a complex pipeline in a modular way, making code easier to maintain and extend. 

So we start by creating a class `TransactionProcessing`, which will be made up of several `Map`, `Filter` and `CombinePerKey` transforms as used previously in the `beam_pipeline.py`. For the sake understanding, I have created an additional pipeline `beam_pipeline_composite.py`, which simply calls `TransactionProcessing()` pipeline between Read/Write, thus seperating the bulk of our processing in the `transaction_processing.py`.  

Bringing everything together, we have
```
import csv
from datetime import datetime
import apache_beam as beam

# The below class extends beam.PTransform, which is the base class. 
# This TransactionProcessing class is for the 'transactions.csv' file, 
# It filters for transaction_amount > 20 and year(timestamp) >= 2010 
# Lastly sums the result, outputting (date,transaction-amount)

class TransactionProcessing(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | beam.Map(lambda line: next(csv.reader([line])))
            | beam.Map(lambda row: tuple(row))  # Convert list to tuple 
            | 'Deduplicate elements' >> beam.Distinct()  # Remove duplicates 
            | beam.Map(lambda row: list(row))  # Convert the tuple back to a list
            | beam.Filter(lambda row: row[0] != 'timestamp')            
            | beam.Filter(lambda row: float(row[3]) > 20)
            | beam.Filter(lambda row: int(row[0].split('-')[0]) >= 2010)
            | beam.Map(lambda row: (datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S %Z').strftime('%Y-%m-%d'), float(row[3])))
            | beam.CombinePerKey(sum)
        )
```

The expand name is used because it is an extension of the PTransform, as part of Beam's conventions and ensure transform works correctly. 


## Step 4 - Testing

Import the necessary modules, in this case we will be using `assert_that` and 
`equal_to` to create our unit test cases. 
```
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
```
Additional modules also imported, including the composite transform class created. 
```
import unittest
import apache_beam as beam
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import unittest.mock
from apache_beam.testing.test_pipeline import TestPipeline
from transforms.transaction_processing import TransactionProcessing
```

We create a testclass as `TransactionProcessingTest`, this will have multiple unit tests. 
- First we will test on a single transaction, 
```
class TransactionProcessingTest(unittest.TestCase):
    # Expected output
    def test_single_transaction(self):
        with TestPipeline(runner='DirectRunner') as p:
            input_data = [
                '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22'
            ]
            expected_output = [
                ('2017-03-18', 2102.22)
            ]
            output = (p 
                        | beam.Create(input_data) 
                        | TransactionProcessing())
            assert_that(output, equal_to(expected_output))
            
```

- Second we will test on filtered conditions, 
```
    # Show filtering for amount > 20 and year < 2010
    def test_filtered_transactions(self):
        with TestPipeline(runner='DirectRunner') as p:
            input_data = [
                '2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',  # Before 2010
                '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95'  # Amount <= 20
            ]
            expected_output = []
            result = p | beam.Create(input_data) | TransactionProcessing()
            assert_that(result, equal_to(expected_output))

```
- Third we will test if it sums on the same date,
```
    # Show summations 
    def test_same_date_transactions(self):
        with TestPipeline(runner='DirectRunner') as p:
            input_data = [
                '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,21.0'
            ]
            expected_output = [
                ('2017-03-18', 2123.22)
            ]
            result = p | beam.Create(input_data) | TransactionProcessing()
            assert_that(result, equal_to(expected_output))
            
```
- Lastly we will test on the sample transaction.csv. 
```
    # Entire transaction.csv process
    def test_process(self):
        with TestPipeline(runner='DirectRunner') as p:
            input_data = [
                '2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',
                '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95',
                '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030',
                '2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08',
                '2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12'
            ]
            expected_output = [
                ('2017-03-18', 2102.22),
                ('2017-08-31', 13700000023.08),
                ('2018-02-27', 129.12)
            ]
            result = p | beam.Create(input_data) | TransactionProcessing()
            assert_that(result, equal_to(expected_output))

```

Run tests using `python3 -m unittest transaction_processing_test.py` 
This will check whether the TransactionProcessing() class is processing the data as expected. 
The lines  `assert_that(output, equal_to(expected_output))` checks whether the output of the pipeline is equal to the expected output, otherwise the test will fail. 