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