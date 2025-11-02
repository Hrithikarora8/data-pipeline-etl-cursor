"""
Extract module - Read CSV files from raw data folder
"""
import pandas as pd
import os
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExtractor:
    def __init__(self, config: Dict):
        self.config = config
        self.raw_path = config['paths']['raw_data']
    
    def extract_sales_data(self) -> pd.DataFrame:
        """Extract sales data from CSV"""
        file_path = os.path.join(self.raw_path, self.config['files']['sales'])
        
        try:
            logger.info(f"Extracting sales data from {file_path}")
            df = pd.read_csv(file_path)
            logger.info(f"✓ Extracted {len(df)} sales records")
            return df
        except Exception as e:
            logger.error(f"✗ Error extracting sales data: {e}")
            raise
    
    def extract_customer_data(self) -> pd.DataFrame:
        """Extract customer data from CSV"""
        file_path = os.path.join(self.raw_path, self.config['files']['customers'])
        
        try:
            logger.info(f"Extracting customer data from {file_path}")
            df = pd.read_csv(file_path)
            logger.info(f"✓ Extracted {len(df)} customer records")
            return df
        except Exception as e:
            logger.error(f"✗ Error extracting customer data: {e}")
            raise
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """Extract all data sources"""
        return {
            'sales': self.extract_sales_data(),
            'customers': self.extract_customer_data()
        }