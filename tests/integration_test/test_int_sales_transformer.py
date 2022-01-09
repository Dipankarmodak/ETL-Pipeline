"""Integration Test XetraETLMethods"""
import io
import unittest
import boto3
import pandas as pd
from pandas._testing import assert_frame_equal
from sales.common.s3 import S3BucketConnector
from sales.transformers.sales_transformers  import SalesETL,SalesSourceConfig, SalesTargetConfig

class IntTestSalesETLMethods(unittest.TestCase):
    """
    Integration testing the SalesETL class.
    """
    def setUp(self):
        """
        Setting up the environment
        """
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID' # access-keys
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY' # secret access key
        self.s3_endpoint_url = 'https://s3.ap-south-1.amazonaws.com'
        self.s3_bucket_name_src = 'sales-int-test-src' # source bucket
        self.s3_bucket_name_trg = 'sales-int-test-trg' # target bucket
        # Creating the source and target bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3',endpoint_url=self.s3_endpoint_url)
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
        # Creating source and target configuration

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
        # Test init
        self.df = pd.read_csv(r'tests\data\sales-report.csv')

    def tearDown(self):
        for key in self.src_bucket.objects.all():
            key.delete()
        for key in self.trg_bucket.objects.all():
            key.delete()

    def test_int_etl_report1(self):
        """
        Integration test for the etl_report1 method
        """
        # Expected results
        df_exp = self.df
        # Method execution
        sales_etl = SalesETL(self.s3_bucket_src, self.s3_bucket_trg,
                             self.source_config, self.target_config)
        sales_etl.etl_report1()
        # Test after method execution
       	target_key_return = [obj for obj in self.trg_bucket.objects.all()][0].key
        data = self.trg_bucket.Object(key=target_key_return).get().get('Body').\
                read().decode('utf-8')
        out_buffer = io.StringIO(data)
        df_result = pd.read_csv(out_buffer)
        assert_frame_equal(df_result,df_exp)

if __name__ == '__main__':
    unittest.main()
