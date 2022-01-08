'Running the Sales-ETL Application'
import argparse
import logging
import logging.config
import yaml
from sales.common.s3 import S3BucketConnector
from sales.transformers.sales_transformers import SalesETL, SalesSourceConfig, SalesTargetConfig
def main():
    "Entry point to run the ETL job"
    # Parsing YML file
    parser = argparse.ArgumentParser(description='Run the Sales ETL Job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    # reading s3 configuration
    s3_config = config['s3']
    # creating the S3BucketConnector classes for source and target
    s3_bucket_src = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['src_endpoint_url'],
                                      bucket=s3_config['src_bucket'])
    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])
    # reading source configuration
    source_config = SalesSourceConfig(**config['source'])
    # reading target configuration
    target_config = SalesTargetConfig(**config['target'])
    logger=logging.getLogger(__name__)
    logger.info('Sales ETL job started..')
    sales_etl = SalesETL(s3_bucket_src, s3_bucket_trg,source_config, target_config)
    # running etl job for xetra report 1
    sales_etl.etl_report1()
    logger.info('Sales ETL job finished..')
if __name__=='__main__':
    main()
