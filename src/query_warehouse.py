from load import DataLoader
import yaml
import os

# Load config
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, 'config/config.yaml')

with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

loader = DataLoader(config)

# Top 5 products by revenue
print("\n📊 Top 5 Products by Revenue:")
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

# Sales by region
print("\n🌍 Sales by Region:")
result = loader.query_warehouse("""
    SELECT * FROM agg_sales_by_region
    ORDER BY total_amount DESC
""")
print(result.to_string(index=False))