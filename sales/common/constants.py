"""
File to store constants
"""

from enum import Enum

class S3FileTypes(Enum):
    """
    Supported file for S3BucketConnector
    """
    CSV='csv'
    