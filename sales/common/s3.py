'''Connector and method accessing s3'''
import logging
import os
import io
import boto3
import pandas as pd
from sales.common.constants import S3FileTypes
from sales.common.custom_exceptions import WrongFormatException

class S3BucketConnector():
    """
    Class for interacting with s3 bucket
    """
    def __init__(self,access_key:str,secret_key:str,endpoint_url:str,bucket:str):
        """Constructor for S3BucketConnector
        :param access_key:access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url:endpoint url to S3
        :param bucket: S3 bucket name
        """
        self._logger=logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3', region_name='ap-south-1',
                                                endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def return_objects(self):
        "returns all the objects present in the defined bucket"
        self._logger.info('endpoint url is %s', self.endpoint_url)
        self._logger.info('Reading file at %s', self._bucket)
        objects = [obj.key for obj in self._bucket.objects.all()]
        return objects

    def read_excel_to_df(self,key):
        "reads the objects from the bucket and returns a dataframe"
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        obj = self._bucket.Object(key=key).get()
        data = pd.read_excel(io.BytesIO(obj['Body'].read()))
        data.reset_index(drop=True,inplace=True)
        return data

    def write_df_to_s3(self,data_frame:pd.DataFrame,key:str,file_format:str):
        "write the files to target s3 bucket"
        if file_format == S3FileTypes.CSV.value:
            out_buffer = io.BytesIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info('The file format %s is not supported to be written to s3!', file_format)
        raise WrongFormatException

    def __put_object(self, out_buffer: io.BytesIO , key: str):
        """
        Helper function for self.write_df_to_s3()
        :out_buffer:  BytesIO that should be written
        :key: target key of the saved file
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
        