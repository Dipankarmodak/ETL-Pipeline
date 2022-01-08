"Sales ETL Component"
from typing import NamedTuple
import logging
import pandas as pd
import numpy as np
from feature_engine.imputation import MeanMedianImputer,RandomSampleImputer,CategoricalImputer
from sales.common.s3 import S3BucketConnector

class SalesSourceConfig(NamedTuple):
    """
    Class for source configuration data
    """
    src_sales_data:str
    src_customer_data:str
    src_customeraddress_data:str
    src_division_data:str
    src_region_data:str
    src_gps_data:str
    src_columns: list
    src_columns2:list
    src_col_date: str
    src_col_address: str
    src_col_custkey: str
    src_col_amount: str
    src_col_costamount: str
    src_col_marginamount: str
    src_col_dis_amount: str
    src_col_salequant: str
    src_col_regioncode: str
    src_col_divison: str
    src_col_city: str
    src_col_state: str
    src_col_country: str
    src_col_item_class: str
    src_col_item: str
    src_col_custype: str
    src_col_gpsaddress:str


class SalesTargetConfig(NamedTuple):
    """
    Class for target configuration data
    """
    trg_col_custkey: str
    trg_col_amount: str
    trg_col_costamount: str
    trg_col_marginamount: str
    trg_col_salequant: str
    trg_col_year: str
    trg_col_month: str
    trg_col_datekey:str
    trg_col_address: str
    trg_col_profper:str
    trg_col_item_class:str
    trg_col_item: str
    trg_col_custype : str
    trg_col_city : str
    trg_col_state: str
    trg_col_region: str
    trg_col_div: str
    trg_col_cogs: str
    trg_col_profit_marg: str
    trg_col_disamount : str
    trg_col_salequantity: str
    trg_col_revenue :str
    trg_key: str
    trg_col_lat: str
    trg_col_lon: str
    trg_format: str


class SalesETL():
    " Reads the Sales data, transforms and writes the transformed to target "

    def __init__(self, s3_bucket_src: S3BucketConnector, s3_bucket_trg: S3BucketConnector,
            src_args: SalesSourceConfig,trg_args: SalesTargetConfig):
        """
        Constructor for SalesTransformer
        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: NamedTouple class with source configuration data
        :param trg_args: NamedTouple class with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.src_args = src_args
        self.trg_args = trg_args

    def extract(self):
        """
               Read the source data and concatenates them to one Pandas DataFrame
               :returns:
                 data_frame: Pandas DataFrame with the extracted data
               """
        self._logger.info('Extracting Sales source files started...')
        dataframes_dictionary = {}
        for obj in self.s3_bucket_src.return_objects():
            dataframes_dictionary[f'{obj}'] = self.s3_bucket_src.read_excel_to_df(obj)
        self._logger.info('Extracting Sales source files finished.')
        return dataframes_dictionary

    def transform_report1(self, dataframe_dict: dict):
        '''Joining, Cleaning of the dataframes happens aswell as report based on year ,month
            and year-month is created'''
        self._logger.info('Applying transformations to Sales source data \
                            for cleaning has started...')
        self._logger.info('Data cleaning has started...')
        sales = dataframe_dict[self.src_args.src_sales_data].set_index(self.\
                src_args.src_col_date)
        region = dataframe_dict[self.src_args.src_region_data]
        customer = dataframe_dict[self.src_args.src_customer_data]
        address = dataframe_dict[self.src_args.src_customeraddress_data]
        divison = dataframe_dict[self.src_args.src_division_data]
        geo_map = dataframe_dict[self.src_args.src_gps_data].\
                  set_index(self.src_args.src_col_gpsaddress)
        mappings = geo_map[['latitude', 'longitude']].to_dict()
        sales.drop(labels=self.src_args.src_columns,axis=1,inplace=True)
        self._logger.info('Unnesscary columns are dropped...')
        sales[self.trg_args.trg_col_custkey] = sales[self.src_args.src_col_custkey].astype(
            'category')
        sales.rename(
            columns={self.src_args.src_col_amount: self.trg_args.trg_col_amount,
                     self.src_args.src_col_costamount: self.trg_args.trg_col_costamount,
                     self.src_args.src_col_marginamount: self.trg_args.trg_col_marginamount},
                     inplace=True)
        sales[self.trg_args.trg_col_year] = pd.DatetimeIndex(sales.index).year
        sales[self.trg_args.trg_col_month] = pd.DatetimeIndex(sales.index).month
        self._logger.info('Missing value imputation begins ....')
        sales_2018= sales[sales[self.trg_args.trg_col_year] == 2018]
        sales.drop(sales[sales[self.trg_args.trg_col_year] == 2018].index, inplace=True)
        sale_2019 = sales[sales.year == 2019].set_index(self.trg_args.trg_col_month)
        sale_2017 = sales[sales.year == 2017].set_index(self.trg_args.trg_col_month)
        sale_2019 = sale_2019[sale_2019.index.isin([i for i in range(4, 13)])]
        sale_2017 = sale_2017[sale_2017.index.isin([i for i in range(4, 13)])]
        report_2019 = pd.pivot_table(sale_2019,
                      values=[self.trg_args.trg_col_costamount,
                      self.trg_args.trg_col_amount,self.src_args.src_col_dis_amount,
                      self.trg_args.trg_col_marginamount,self.src_args.src_col_salequant],
                      index=self.trg_args.trg_col_month, aggfunc=sum)
        report_2017 = pd.pivot_table(sale_2017,
                      values=[self.trg_args.trg_col_costamount, self.trg_args.trg_col_amount,
                      self.src_args.src_col_dis_amount,self.trg_args.trg_col_marginamount,
                      self.src_args.src_col_salequant],index=self.trg_args.trg_col_month,
                      aggfunc=sum)
        df_concat = pd.concat((report_2019, report_2017))
        df_means = df_concat.groupby(level=0).mean()
        df_means[self.trg_args.trg_col_salequant] = \
        df_means[self.src_args.src_col_salequant].astype('int64')
        idx = pd.date_range('2018-04-30', '2018-12-31', freq='M')
        df_means.set_index(idx, inplace=True)
        new_2018 = pd.concat((sales_2018, df_means))
        new_2018[self.trg_args.trg_col_year] = pd.DatetimeIndex(new_2018.index).year
        new_2018[self.trg_args.trg_col_month] = pd.DatetimeIndex(new_2018.index).month
        cst_key = pd.pivot_table(sales_2018,
                  values=[self.trg_args.trg_col_costamount, self.trg_args.trg_col_amount,
                  self.trg_args.trg_col_marginamount, self.src_args.src_col_salequant],
                  index=[self.src_args.src_col_custkey]).sort_values(self.trg_args.trg_col_amount,
                  ascending=False).index[0]
        new_2018[self.src_args.src_col_custkey] = \
            new_2018[self.src_args.src_col_custkey].fillna(cst_key)
        complete_df = pd.concat((sales, new_2018))
        new_data = pd.merge(customer, address, on=self.src_args.src_col_address, how='left')
        new_data1 = pd.merge(new_data, region, on=self.src_args.src_col_regioncode, how='left')
        new_data1.rename({'Customer Number': self.src_args.src_col_custkey}, inplace=True, axis=1)
        new_data2 = pd.merge(complete_df, new_data1, on=self.src_args.src_col_custkey, how='left')
        new_data3 = pd.merge(new_data2, divison, on=self.src_args.src_col_divison, how='left')
        new_data3.replace(to_replace=' ', value=np.nan, inplace=True)
        new_data3[self.trg_args.trg_col_address] = \
             new_data3[self.src_args.src_col_city] + ',' + new_data3[self.src_args.src_col_state] \
             + ',' + new_data3[self.src_args.src_col_country]
        new_data3[self.trg_args.trg_col_profper] = \
            (new_data3['Profit Amount'] / new_data3['Revenue']) * 100
        new_data3.drop(
            columns=self.src_args.src_columns2, inplace=True)
        median_imputer = MeanMedianImputer(imputation_method='median',
        variables=[self.src_args.src_col_dis_amount])
        median_imputer.fit(new_data3[new_data3[self.trg_args.trg_col_year] == 2017])
        new_data3[new_data3[self.trg_args.trg_col_year] == 2017] = median_imputer.transform(
            new_data3[new_data3[self.trg_args.trg_col_year] == 2017])
        cat_imputer = CategoricalImputer(imputation_method='missing', fill_value='Missing',
                        variables=[self.src_args.src_col_item_class, self.src_args.src_col_item])
        new_data3 = cat_imputer.fit_transform(new_data3)
        imputer_1 = RandomSampleImputer(
            random_state=[self.trg_args.trg_col_amount, self.trg_args.trg_col_costamount,
            self.trg_args.trg_col_profper,self.src_args.src_col_salequant,
            self.src_args.src_col_dis_amount],seed='observation',
            seeding_method='add', variables=[self.src_args.src_col_city,
            self.src_args.src_col_state, self.trg_args.trg_col_address]
        )
        imputer_2 = RandomSampleImputer(
            random_state=[self.trg_args.trg_col_amount, self.trg_args.trg_col_costamount,
            self.trg_args.trg_col_profper,self.src_args.src_col_salequant,
            self.src_args.src_col_dis_amount],seed='observation',
            seeding_method='add',variables=[self.src_args.src_col_city, self.src_args.src_col_state,
            self.trg_args.trg_col_address]
        )
        imputer_3 = RandomSampleImputer(
            random_state=[self.trg_args.trg_col_amount, self.trg_args.trg_col_costamount,
            self.trg_args.trg_col_profper,
            self.src_args.src_col_salequant, self.src_args.src_col_dis_amount],
            seed='observation',seeding_method='add',variables=[self.src_args.src_col_city,
            self.src_args.src_col_state, self.trg_args.trg_col_address]
        )
        imputer_1.fit(new_data3[new_data3[self.trg_args.trg_col_year] == 2017])
        imputer_2.fit(new_data3[new_data3[self.trg_args.trg_col_year] == 2018])
        imputer_3.fit(new_data3[new_data3[self.trg_args.trg_col_year] == 2019])
        new_data3[new_data3[self.trg_args.trg_col_year] == 2017] = imputer_1.transform(
            new_data3[new_data3[self.trg_args.trg_col_year] == 2017])
        new_data3[new_data3[self.trg_args.trg_col_year] == 2018] = imputer_2.transform(
            new_data3[new_data3[self.trg_args.trg_col_year] == 2018])
        new_data3[new_data3[self.trg_args.trg_col_year] == 2019] = imputer_3.transform(
            new_data3[new_data3[self.trg_args.trg_col_year] == 2019])
        new_data3[self.trg_args.trg_col_year] = \
            new_data3[self.trg_args.trg_col_year].astype('category')
        new_data3[self.trg_args.trg_col_month] = \
            new_data3[self.trg_args.trg_col_month].astype('category')
        mappings = geo_map[['latitude', 'longitude']].to_dict()
        new_data3[self.trg_args.trg_col_lat] = \
            new_data3[self.trg_args.trg_col_address].map(mappings['latitude'])
        new_data3[self.trg_args.trg_col_lon] = \
            new_data3[self.trg_args.trg_col_address].map(mappings['longitude'])
        self._logger.info('latitude and longitutde has been inserted...')
        new_data3.drop(columns=self.trg_args.trg_col_address, inplace=True)
        new_data3.drop_duplicates(inplace=True)
        new_data3[self.trg_args.trg_col_datekey]= \
                 new_data3[self.trg_args.trg_col_year].astype('str') \
                 +new_data3[self.trg_args.trg_col_month].astype('str')
        new_data3[self.trg_args.trg_col_datekey]=\
            pd.to_datetime(new_data3[self.trg_args.trg_col_datekey],format='%Y%m')
        new_data3.drop(axis=1,columns=[self.trg_args.trg_col_year,
                self.trg_args.trg_col_month],inplace=True)
        self._logger.info('Data cleaning has finished...')
        self._logger.info('Applying transformations to Sales source \
        data for cleaning has finished...')
        self._logger.info('Loading the clean data to S3 bucket....')
        return new_data3

    def load(self, data_frame: pd.DataFrame):
        """
       Saves a Pandas DataFrame to the target
       :param data_frame: Pandas DataFrame as Input
               """
        # Creating target key
        target_key = f'{self.trg_args.trg_key}.{self.trg_args.trg_format}'
        # Writing to target
        self.s3_bucket_trg.write_df_to_s3(data_frame, target_key,self.trg_args.trg_format)
        self._logger.info('Sales target data successfully written.')
        self._logger.info('ETL process terminated...')
        return True

    def etl_report1(self):
        """
        Extract, transform and load to create report...
        """
        # Extraction
        dictionary = self.extract()
        # Transformation
        data_frame = self.transform_report1(dictionary)
        # Load
        self.load(data_frame)
        return True
