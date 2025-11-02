"""
Main ETL Pipeline - Orchestrates Extract, Transform, Load
"""
import yaml
import logging
from datetime import datetime
from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    def __init__(self, config_path='config/config.yaml'):
        """Initialize ETL pipeline with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.extractor = DataExtractor(self.config)
        self.transformer = DataTransformer(self.config)
        self.loader = DataLoader(self.config)
    
    def run(self):
        """Execute the complete ETL pipeline"""
        start_time = datetime.now()
        logger.info("="*60)
        logger.info("Starting ETL Pipeline")
        logger.info("="*60)
        
        try:
            # EXTRACT
            logger.info("\n[EXTRACT] Reading raw data...")
            raw_data = self.extractor.extract_all()
            
            # TRANSFORM
            logger.info("\n[TRANSFORM] Cleaning and transforming data...")
            enriched_sales, aggregations = self.transformer.transform_all(raw_data)
            
            # LOAD
            logger.info("\n[LOAD] Loading data into warehouse...")
            self.loader.load_all(enriched_sales, aggregations)
            
            # Summary
            duration = (datetime.now() - start_time).total_seconds()
            logger.info("\n" + "="*60)
            logger.info(f"✓ ETL Pipeline completed successfully in {duration:.2f} seconds")
            logger.info("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"\n✗ Pipeline failed: {e}")
            return False


def main():
    """Main entry point"""
    pipeline = ETLPipeline()
    success = pipeline.run()
    
    if success:
        # Example: Query the warehouse
        logger.info("\n[EXAMPLE QUERY] Top 5 products by revenue:")
        loader = DataLoader(pipeline.config)
        result = loader.query_warehouse("""
            SELECT product, 
                   SUM(total_amount) as total_revenue,
                   SUM(quantity) as total_quantity
            FROM fact_sales
            GROUP BY product
            ORDER BY total_revenue DESC
            LIMIT 5
        """)
        print(result.to_string(index=False))


if __name__ == "__main__":
    main()