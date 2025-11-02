"""
Load module - Load data into DuckDB warehouse
"""
import duckdb
import pandas as pd
from typing import Dict
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self, config: Dict):
        self.config = config
        self.db_path = config['paths']['warehouse']
        self.conn = None
    
    def connect(self):
        """Connect to DuckDB database"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        logger.info(f"✓ Connected to warehouse: {self.db_path}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("✓ Closed warehouse connection")
    
    def load_fact_sales(self, df: pd.DataFrame):
        """Load sales fact table"""
        logger.info("Loading sales fact table...")
        
        table_name = self.config['warehouse']['fact_table']
        
        # Drop table if exists and create new
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create table from dataframe
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
        count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"✓ Loaded {count} records into {table_name}")
    
    def load_aggregations(self, aggregations: Dict[str, pd.DataFrame]):
        """Load aggregation tables"""
        logger.info("Loading aggregation tables...")
        
        for name, df in aggregations.items():
            table_name = f"agg_{name}"
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"  - Loaded {count} records into {table_name}")
        
        logger.info(f"✓ Loaded {len(aggregations)} aggregation tables")
    
    def save_to_csv(self, df: pd.DataFrame, filename: str):
        """Save processed data to CSV"""
        output_path = os.path.join(self.config['paths']['processed_data'], filename)
        os.makedirs(self.config['paths']['processed_data'], exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"✓ Saved processed data to {output_path}")
    
    def load_all(self, enriched_sales: pd.DataFrame, aggregations: Dict[str, pd.DataFrame]):
        """Load all data into warehouse"""
        self.connect()
        
        try:
            # Load fact table
            self.load_fact_sales(enriched_sales)
            
            # Load aggregations
            self.load_aggregations(aggregations)
            
            # Save processed data to CSV as backup
            self.save_to_csv(enriched_sales, 'enriched_sales.csv')
            
            logger.info("✓ All data loaded successfully")
            
        except Exception as e:
            logger.error(f"✗ Error loading data: {e}")
            raise
        finally:
            self.close()
    
    def query_warehouse(self, query: str):
        """Execute a query on the warehouse"""
        self.connect()
        try:
            result = self.conn.execute(query).fetchdf()
            return result
        finally:
            self.close()