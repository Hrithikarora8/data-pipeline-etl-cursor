"""
Transform module - Clean and transform data
"""
import pandas as pd
from typing import Dict, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer:
    def __init__(self, config: Dict):
        self.config = config
        self.quality_rules = config['quality_rules']
    
    def clean_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean sales data"""
        logger.info("Cleaning sales data...")
        
        # Create a copy
        df_clean = df.copy()
        
        # Remove records with missing critical fields
        initial_count = len(df_clean)
        df_clean = df_clean.dropna(subset=['order_id', 'date', 'product'])
        logger.info(f"  - Removed {initial_count - len(df_clean)} records with missing critical fields")
        
        # Handle missing customer_id (fill with 'UNKNOWN')
        missing_customers = df_clean['customer_id'].isna().sum()
        df_clean['customer_id'] = df_clean['customer_id'].fillna('UNKNOWN')
        if missing_customers > 0:
            logger.info(f"  - Filled {missing_customers} missing customer IDs with 'UNKNOWN'")
        
        # Fix negative or invalid prices
        invalid_prices = (df_clean['unit_price'] < self.quality_rules['min_price']).sum()
        df_clean.loc[df_clean['unit_price'] < self.quality_rules['min_price'], 'unit_price'] = df_clean['unit_price'].median()
        if invalid_prices > 0:
            logger.info(f"  - Fixed {invalid_prices} invalid prices with median value")
        
        # Convert date to datetime
        df_clean['date'] = pd.to_datetime(df_clean['date'])
        
        # Calculate total amount
        df_clean['total_amount'] = df_clean['quantity'] * df_clean['unit_price']
        
        logger.info(f"✓ Cleaned sales data: {len(df_clean)} records ready")
        return df_clean
    
    def clean_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean customer data"""
        logger.info("Cleaning customer data...")
        
        df_clean = df.copy()
        
        # Remove duplicates
        initial_count = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['customer_id'])
        duplicates = initial_count - len(df_clean)
        if duplicates > 0:
            logger.info(f"  - Removed {duplicates} duplicate customer records")
        
        # Convert signup_date to datetime
        df_clean['signup_date'] = pd.to_datetime(df_clean['signup_date'])
        
        logger.info(f"✓ Cleaned customer data: {len(df_clean)} records ready")
        return df_clean
    
    def enrich_sales_data(self, sales_df: pd.DataFrame, customer_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich sales data with customer information"""
        logger.info("Enriching sales data with customer information...")
        
        enriched_df = sales_df.merge(
            customer_df[['customer_id', 'name', 'customer_type']],
            on='customer_id',
            how='left'
        )
        
        # Fill missing customer names for UNKNOWN customers
        enriched_df['name'] = enriched_df['name'].fillna('Unknown Customer')
        enriched_df['customer_type'] = enriched_df['customer_type'].fillna('Standard')
        
        logger.info(f"✓ Enriched {len(enriched_df)} sales records")
        return enriched_df
    
    def create_aggregations(self, enriched_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create useful aggregations"""
        logger.info("Creating aggregations...")
        
        aggregations = {}
        
        # Sales by region
        aggregations['sales_by_region'] = enriched_df.groupby('region').agg({
            'total_amount': 'sum',
            'quantity': 'sum',
            'order_id': 'count'
        }).rename(columns={'order_id': 'order_count'}).reset_index()
        
        # Sales by product
        aggregations['sales_by_product'] = enriched_df.groupby('product').agg({
            'total_amount': 'sum',
            'quantity': 'sum',
            'order_id': 'count'
        }).rename(columns={'order_id': 'order_count'}).reset_index()
        
        # Sales by customer type
        aggregations['sales_by_customer_type'] = enriched_df.groupby('customer_type').agg({
            'total_amount': 'sum',
            'quantity': 'sum',
            'order_id': 'count'
        }).rename(columns={'order_id': 'order_count'}).reset_index()
        
        # Daily sales
        enriched_df['date_only'] = enriched_df['date'].dt.date
        aggregations['daily_sales'] = enriched_df.groupby('date_only').agg({
            'total_amount': 'sum',
            'order_id': 'count'
        }).rename(columns={'order_id': 'order_count'}).reset_index()
        
        logger.info(f"✓ Created {len(aggregations)} aggregation tables")
        return aggregations
    
    def transform_all(self, raw_data: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Execute all transformations"""
        # Clean data
        clean_sales = self.clean_sales_data(raw_data['sales'])
        clean_customers = self.clean_customer_data(raw_data['customers'])
        
        # Enrich sales with customer data
        enriched_sales = self.enrich_sales_data(clean_sales, clean_customers)
        
        # Create aggregations
        aggregations = self.create_aggregations(enriched_sales)
        
        return enriched_sales, aggregations