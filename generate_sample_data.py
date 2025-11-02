import csv
import random
from datetime import datetime, timedelta

# Generate sample sales data
def generate_sales_data(filename, num_records=1000):
    products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Webcam', 'Headphones', 'USB Cable', 'Charger']
    regions = ['North', 'South', 'East', 'West']
    customers = [f'CUST{str(i).zfill(4)}' for i in range(1, 101)]
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['order_id', 'date', 'customer_id', 'product', 'quantity', 'unit_price', 'region', 'status'])
        
        start_date = datetime(2024, 1, 1)
        
        for i in range(1, num_records + 1):
            order_id = f'ORD{str(i).zfill(6)}'
            date = start_date + timedelta(days=random.randint(0, 300))
            customer_id = random.choice(customers)
            product = random.choice(products)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(10.0, 500.0), 2)
            region = random.choice(regions)
            status = random.choice(['Completed', 'Pending', 'Cancelled', 'Completed', 'Completed'])  # More completed
            
            # Introduce some data quality issues for ETL practice
            if i % 50 == 0:  # Add some nulls
                customer_id = ''
            if i % 100 == 0:  # Add some invalid prices
                unit_price = -10.0
            
            writer.writerow([order_id, date.strftime('%Y-%m-%d'), customer_id, product, quantity, unit_price, region, status])

# Generate sample customer data
def generate_customer_data(filename, num_records=100):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['customer_id', 'name', 'email', 'phone', 'signup_date', 'customer_type'])
        
        for i in range(1, num_records + 1):
            customer_id = f'CUST{str(i).zfill(4)}'
            name = f'Customer {i}'
            email = f'customer{i}@example.com'
            phone = f'+1-555-{str(random.randint(1000000, 9999999))}'
            signup_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
            customer_type = random.choice(['Premium', 'Standard', 'Basic'])
            
            writer.writerow([customer_id, name, email, phone, signup_date.strftime('%Y-%m-%d'), customer_type])

# Generate the files
print("Generating sample data...")
generate_sales_data('sales_data.csv', 1000)
generate_customer_data('customer_data.csv', 100)
print("✓ Generated sales_data.csv (1000 records)")
print("✓ Generated customer_data.csv (100 records)")
print("\nData includes some quality issues for ETL practice:")
print("  - Missing customer IDs")
print("  - Negative prices")
print("  - Various statuses")