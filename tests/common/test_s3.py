"""TestS3BucketConnectorMethods"""
import os
import io
import unittest
import boto3
import pandas as pd
from moto import mock_s3
from sales.common.s3 import S3BucketConnector
from sales.common.custom_exceptions import WrongFormatException
class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector class.
    """
    def setUp(self):
        """
        Setting up the environment
        """
        # mocking s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.ap-south-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating a bucket on the mocket s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'ap-south-1'})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
    def tearDown(self):
        # mocking s3 connection stop
        self.mock_s3.stop()
    def test_return_objects(self):
        """
        Tests the list_files_in_prefix method for getting
        the 2 file keys as list on the mocket s3 bucket
        """
        # Expected results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.xlsx'
        # Test init
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        # Method execution
        list_result = self.s3_bucket_conn.return_objects()
        # Test after method execution
        self.assertEqual(len(list_result), 1)
        self.assertIn(key1_exp, list_result)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                ]
            }
        )
    def test_read_excel_to_df_ok(self):
        """
        Tests the read_csv_to_df method for
        reading 1 .csv file from the mocked s3 bucket
        """
        # Expected results
        key_exp = 'testinput.xls'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        # Test init
        #  Created an Excel file
        log_exp = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        data_frame=pd.DataFrame({'col1':['val1'],'col2':['val2']})
        with io.BytesIO() as output:
                with pd.ExcelWriter(output,engine='openpyxl') as writer:
                    data_frame.to_excel(writer,sheet_name='Sheet_1',index=False)
                data = output.getvalue()
        # Uploading the Excel file to mock S3
        self.s3_bucket.put_object(Key=key_exp, Body=data)
        # Method execution
        with self.assertLogs() as logm:
            df_result = self.s3_bucket_conn.read_excel_to_df(key_exp)
            self.assertIn(log_exp, logm.output[0])
        # Test after method execution
        self.assertEqual(df_result.shape[0], 1)
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_exp, df_result[col1_exp][0])
        self.assertEqual(val2_exp, df_result[col2_exp][0])
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )
    def test_write_df_to_s3_excel(self):
        """
        Tests the write_df_to_s3 method
        if writing excel is successful
        """
        # Expected results
        return_exp = True
        df_exp1 = pd.read_excel(r'tests\data\sales-report.xlsx',sheet_name='Sheet_name_1',
        engine='openpyxl')
        df_exp2 = pd.read_excel(r'tests\data\sales-report.xlsx',sheet_name='Sheet_name_2',
        engine='openpyxl')
        df_exp3 = pd.read_excel(r'tests\data\sales-report.xlsx',sheet_name='Sheet_name_3',
        engine='openpyxl')
        df_exp4 = pd.read_excel(r'tests\data\sales-report.xlsx',sheet_name='Sheet_name_3',
        engine='openpyxl')
        key_exp = 'test.xlsx'
        # Test init
        file_format = 'xlsx'
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_exp1,df_exp2,df_exp3,df_exp4,
            key_exp, file_format)
            self.assertIn(log_exp, logm.output[0])
        # Log test after method execution
        #target_key = f'{key_exp}.'f'{file_format}'
        # Test after method execution
        obj = self.s3_bucket.Object(key=key_exp).get()
        df_result = pd.read_excel(io.BytesIO(obj['Body'].read()),sheet_name='Sheet_name_1',
        engine='openpyxl')
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp1.equals(df_result))
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )
    def test_write_df_to_s3_wrong_format(self):
        """  Tests the write_df_to_s3 method
        if a not supported format is given as argument
        """
        # Expected results
        df_exp1 = pd.read_excel(r'tests\data\sales-report.xlsx',sheet_name='Sheet_name_1',
        engine='openpyxl')
        df_exp2 = pd.read_excel(r'tests\data\sales-report.xlsx',sheet_name='Sheet_name_2',
        engine='openpyxl')
        df_exp3 = pd.read_excel(r'tests\data\sales-report.xlsx',sheet_name='Sheet_name_3',
        engine='openpyxl')
        df_exp4 = pd.read_excel(r'tests\data\sales-report.xlsx',sheet_name='Sheet_name_3',
        engine='openpyxl')
        key_exp = 'test.xlsx'
        format_exp = 'wrong_format'
        log_exp = f'The file format {format_exp} is not supported to be written to s3!'
        exception_exp = WrongFormatException
        # Method execution
        with self.assertLogs() as logm:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_to_s3(df_exp1, df_exp2,df_exp3,df_exp4,
                key_exp, format_exp)
            self.assertIn(log_exp, logm.output[0])
