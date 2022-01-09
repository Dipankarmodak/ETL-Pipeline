"""TestXetraETLMethods"""
import os
import unittest
import io
import boto3
import pandas as pd
from pandas._testing import assert_frame_equal
from moto import mock_s3
from sales.common.s3 import S3BucketConnector
from sales.transformers.sales_transformers  import SalesETL,SalesSourceConfig, SalesTargetConfig

class TestSalesETLMethods(unittest.TestCase):
    """
    Testing the SalesETL class.
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
        self.s3_bucket_name_src = 'src-bucket'
        self.s3_bucket_name_trg = 'trg-bucket'
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating the source and target bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name_src,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'ap-south-1'})
        self.s3.create_bucket(Bucket=self.s3_bucket_name_trg,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'ap-south-1'})
        self.src_bucket = self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket = self.s3.Bucket(self.s3_bucket_name_trg)
        # Creating S3BucketConnector testing instances
        self.s3_bucket_src = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name_src)
        self.s3_bucket_trg = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name_trg)
        conf_dict_src = {
              'src_sales_data': 'SALESDATA.xls',
              'src_customer_data': 'CUSTOMERS.xls',
              'src_customeraddress_data': 'CUSTOMERADDRESS.xls',
              'src_division_data': 'DIVISION.xls',
              'src_region_data': 'REGION.xls',
              'src_gps_data': 'GPS.xls',
              'src_columns': ['U/M', 'Unnamed: 20', 'Unnamed: 21', 'Sales Price', 'Order Number',
              'Line Number', 'Invoice Number','Item Number', 'Invoice Date', 'List Price',
              'Promised Delivery Date','Sales Amount Based on List Price','Sales Rep'],
              'src_columns2': ['Customer Address 1','Customer Address 2','Customer Address 3',
              'Customer Address 4','Zip Code','Division','Region Code','Phone','CustKey',
              'Country', 'Search Type','Profit Amount','Business Unit','Line of Business',
              'Business Family','Address Number','Customer','Regional Sales Mgr'],
              'src_col_custkey': 'CustKey',
              'src_col_amount': 'Sales Amount',
              'src_col_costamount': 'Sales Cost Amount',
              'src_col_marginamount': 'Sales Margin Amount',
              'src_col_dis_amount': 'Discount Amount',
              'src_col_salequant': 'Sales Quantity',
              'src_col_address': 'Address Number',
              'src_col_regioncode': 'Region Code',
              'src_col_divison': 'Division',
              'src_col_city': 'City',
              'src_col_state': 'State',
              'src_col_country': 'Country',
              'src_col_item_class': 'Item Class',
              'src_col_item': 'Item',
              'src_col_custype': 'Customer Type',
              'src_col_date' :'DateKey',
              'src_col_gpsaddress' : 'Address'
        }

        conf_dict_trg = {
                'trg_col_custkey': 'CustKey',
                'trg_col_amount': 'Revenue',
                'trg_col_costamount': 'COGS',
                'trg_col_marginamount': 'Profit Amount',
                'trg_col_year': 'year',
                'trg_col_month': 'month',
                'trg_col_datekey': 'Datekey',
                'trg_col_salequant': 'Sales Quantity',
                'trg_col_address': 'Address',
                'trg_col_profper': 'Profit_Margin_%',
                'trg_col_item_class': 'Item_Class',
                'trg_col_item': 'Item',
                'trg_col_custype': 'Customer_Type',
                'trg_col_city': 'City',
                'trg_col_state': 'State',
                'trg_col_region': 'Region_Name',
                'trg_col_div': 'Division',
                'trg_col_cogs': 'COGS',
                'trg_col_profit_marg': 'Profit_Margin',
                'trg_col_disamount': 'Discount_Amt',
                'trg_col_salequantity': 'Sales_Quantity',
                'trg_col_revenue': 'Revenue',
                'trg_col_lat': 'lat',
                'trg_col_lon': 'lon',
                'trg_key': 'sales-report',
                'trg_format': 'csv'
        }
        self.source_config = SalesSourceConfig(**conf_dict_src)
        self.target_config = SalesTargetConfig(**conf_dict_trg)
        # Creating SalesETL testing instance
        # Creating source files on mocked s3
        self.columns=['Invoice Date','List Price','Sales Amount','Sales Amount Based on \
            List Price','Sales Cost Amount','Sales Margin Amount','Sales Price',
            'Sales Quantity','Sales Rep','U/M']
        self.df=pd.read_csv(r'tests\data\sales-report.csv')

    def tearDown(self):
        # mocking s3 connection stop
        self.mock_s3.stop()

    def test_extract_xls(self):
        """
        Tests the extract method when
        there are files to be extracted
        """
        # Expected results
        shape_exp = 4
        # Test init
        self.src_bucket.upload_file(r'tests\data\REGION.xls', self.source_config.src_region_data)
        self.src_bucket.upload_file(r'tests\data\CUSTOMERS.xls',
                                    self.source_config.src_customer_data)
        self.src_bucket.upload_file(r'tests\data\CUSTOMERADDRESS.xls',
                                        self.source_config.src_customeraddress_data)
        self.src_bucket.upload_file(r'tests\data\DIVISION.xls',
                                        self.source_config.src_division_data)
        # Method execution
        sales_etl = SalesETL(self.s3_bucket_src,self.s3_bucket_trg,
                                  self.source_config, self.target_config)
        df_return = sales_etl.extract()
        # Tests after method execution
        self.assertEqual(shape_exp, len(df_return))

    def test_transform_report1_ok(self):
        """
        Tests the transform_report1 method with
        an DataFrame as input argument
        """
        sales= pd.read_excel(r'tests\data\SALESDATA.xls')
        region=pd.read_excel(r'tests\data\REGION.xls')
        customer = pd.read_excel(r'tests\data\CUSTOMERS.xls')
        address= pd.read_excel(r'tests\data\CUSTOMERADDRESS.xls')
        division = pd.read_excel(r'tests\data\DIVISION.xls')
        geo = pd.read_excel(r'tests\data\GPS.xls')
        # Expected results
        df_shape=self.df.shape
        new_dict = {self.source_config.src_sales_data: sales, self.source_config.\
                    src_region_data: region,self.source_config.src_gps_data: \
                    geo,self.source_config.src_customer_data: \
                    customer, self.source_config.src_customeraddress_data: \
                    address,self.source_config.src_division_data: division}
        # Method execution
        sales_etl = SalesETL(self.s3_bucket_src, self.s3_bucket_trg,
                             self.source_config, self.target_config)
        df_result = sales_etl.transform_report1(new_dict)
        # Test after method execution
        self.assertEqual(df_shape, df_result.shape)

    def test_load_ok(self):
        """
        Tests the load method
        """
        self.src_bucket.upload_file(r'tests\data\SALESDATA.xls',
            self.source_config.src_sales_data)
        self.src_bucket.upload_file(r'tests\data\REGION.xls',
            self.source_config.src_region_data)
        self.src_bucket.upload_file(r'tests\data\CUSTOMERS.xls',
            self.source_config.src_customer_data)
        self.src_bucket.upload_file(r'tests\data\CUSTOMERADDRESS.xls',
            self.source_config.src_customeraddress_data)
        self.src_bucket.upload_file(r'tests\data\DIVISION.xls',
            self.source_config.src_division_data)
        self.src_bucket.upload_file(r'tests\data\GPS.xls',
            self.source_config.src_gps_data)
        # Expected results
        # The first thing you can test if the key of the uploaded file is correct
        target_key_exp = f'{self.target_config.trg_key}.{self.target_config.trg_format}'
        df_exp1 = self.df
        # Method execution
        sales_etl = SalesETL(self.s3_bucket_src, self.s3_bucket_trg,
                             self.source_config, self.target_config)
        sales_etl.load(df_exp1)
        # Processing after method execution
        target_key_return = [obj for obj in self.trg_bucket.objects.all()][0].key
        data = self.trg_bucket.Object(key=target_key_return).get().get('Body').\
                read().decode('utf-8')
        out_buffer = io.StringIO(data)
        df_result = pd.read_csv(out_buffer)
        # Test after method execution
        self.assertEqual(target_key_exp, target_key_return)
        assert_frame_equal(df_result,df_exp1)
        # Here you have to check what you expect and what you want and how to read this excel file
        # should look like

    def test_etl_report(self):
        """
        Tests the etl_report1 method
        """
        target_key_exp = f'{self.target_config.trg_key}.{self.target_config.trg_format}'
        # Expected results
        df_exp = self.df
        # Test init
        self.src_bucket.upload_file(r'tests\data\SALESDATA.xls',
        'SALESDATA.xls')
        self.src_bucket.upload_file(r'tests\data\REGION.xls',
        'REGION.xls')
        self.src_bucket.upload_file(r'tests\data\CUSTOMERS.xls',
        'CUSTOMERS.xls')
        self.src_bucket.upload_file(r'tests\data\CUSTOMERADDRESS.xls',
        'CUSTOMERADDRESS.xls')
        self.src_bucket.upload_file(r'tests\data\DIVISION.xls',
        'DIVISION.xls')
        self.src_bucket.upload_file(r'tests\data\GPS.xls',
        'GPS.xls')
        # Method execution
        sales_etl = SalesETL(self.s3_bucket_src, self.s3_bucket_trg,
                             self.source_config, self.target_config)
        sales_etl.etl_report1()
        # Test after method execution
        target_key_return = [obj for obj in self.trg_bucket.objects.all()][0].key
        self.assertEqual(target_key_exp, target_key_return)
        data = self.trg_bucket.Object(key=target_key_return).get().get('Body').\
                read().decode('utf-8')
        out_buffer = io.StringIO(data)
        df_result = pd.read_csv(out_buffer)
        assert_frame_equal(df_result,df_exp)
        # Cleanup after test
        self.trg_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': target_key_return
                    },
                    {
                        'Key': target_key_return
                    }
                ]
            }
        )

if __name__ == '__main__':
    unittest.main()
