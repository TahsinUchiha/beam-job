import unittest
import apache_beam as beam
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import unittest.mock
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from transforms.transaction_processing import TransactionProcessing

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
            print('Single Test: ')
            print('Input: ', input_data)
            print('Expected Output: ', expected_output)
            
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
            print('Filtered Transaction Test: ')
            print('Input: ', input_data)
            print('Expected Output: ', expected_output)

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
            print('Same data Test: ')
            print('Input: ', input_data)
            print('Expected Output: ', expected_output)

    # Duplicate transaction tests 
    def test_duplicate_transactions(self):
        with TestPipeline(runner='DirectRunner') as p:
            input_data = [
                '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,21.0'
            ]
            expected_output = [
                ('2017-03-18', 2123.22)
            ]
            result = p | beam.Create(input_data) | TransactionProcessing()
            assert_that(result, equal_to(expected_output))
            print('Duuplicate data Test: ')
            print('Input: ', input_data)
            print('Expected Output: ', expected_output)

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
            print('Full sample Test data: ')
            print('Input: ', input_data)
            print('Expected Output: ', expected_output)
if __name__ == '__main__':
    unittest.main()