"""
Script to generate sample real estate data for testing.

Generates a CSV file with 100,000+ property records including:
- Valid records
- Corrupted location codes
- Missing prices
- Invalid square meters
- Duplicate records
- Various property types
"""

import csv
import random
from datetime import datetime, timedelta
import os

# Configuration
OUTPUT_FILE = './data/real_estate_data.csv'
NUM_RECORDS = 100000

# Valid data samples
CITIES = [
    'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
    'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
    'Austin', 'Jacksonville', 'Denver', 'Boston', 'Seattle'
]

PROPERTY_TYPES = ['residential', 'commercial', 'industrial', 'land']
SELLER_TYPES = ['private', 'developer', 'institutional']

# Generate location codes based on cities
LOCATION_CODE_MAP = {
    'New York': 'NYC-001',
    'Los Angeles': 'LAC-001',
    'Chicago': 'CHI-001',
    'Houston': 'HOU-001',
    'Phoenix': 'PHX-001',
    'Philadelphia': 'PHL-001',
    'San Antonio': 'SAT-001',
    'San Diego': 'SAN-001',
    'Dallas': 'DAL-001',
    'San Jose': 'SJC-001',
    'Austin': 'AUS-001',
    'Jacksonville': 'JAX-001',
    'Denver': 'DEN-001',
    'Boston': 'BOS-001',
    'Seattle': 'SEA-001'
}

VALID_LOCATION_CODES = list(LOCATION_CODE_MAP.values())


def generate_date(days_back=1825):
    """Generate a random date within the last 5 years."""
    days = random.randint(0, days_back)
    return (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')


def generate_record(record_id):
    """Generate a single property record with optional corruptions for testing."""
    city = random.choice(CITIES)
    location_code = LOCATION_CODE_MAP[city]
    
    # Randomly introduce data quality issues for testing (about 5-10% of records)
    quality_issue = random.random()
    
    if quality_issue < 0.02:
        # Corrupted location code (2% of records)
        location_code = f"CORRUPT_{random.randint(1000, 9999)}"
    elif quality_issue < 0.03:
        # Missing price (1% of records)
        sale_price = ''
    elif quality_issue < 0.05:
        # Invalid square meters (2% of records)
        square_meters = random.choice([-100, 0, 200000])
    else:
        # Valid record
        sale_price = round(random.uniform(50000, 5000000), 2)
        square_meters = round(random.uniform(50, 50000), 2)
    
    # Set defaults for all quality issues
    if quality_issue >= 0.02:
        if not isinstance(sale_price, (int, float)):
            sale_price = round(random.uniform(50000, 5000000), 2)
    
    if quality_issue >= 0.05:
        if not isinstance(square_meters, (int, float)):
            square_meters = round(random.uniform(50, 50000), 2)
    
    property_id = f"PROP_{record_id:08d}"
    address = f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Maple', 'Pine'])} St"
    sale_date = generate_date()
    property_type = random.choice(PROPERTY_TYPES)
    seller_type = random.choice(SELLER_TYPES)
    
    return {
        'property_id': property_id,
        'address': address,
        'city': city,
        'location_code': location_code,
        'sale_price': sale_price,
        'sale_date': sale_date,
        'square_meters': square_meters,
        'property_type': property_type,
        'seller_type': seller_type
    }


def main():
    """Generate sample CSV file."""
    # Create data directory if it doesn't exist
    os.makedirs('./data', exist_ok=True)
    
    print(f"Generating {NUM_RECORDS} sample property records...")
    
    # Generate records
    records = []
    duplicate_count = 0
    
    for i in range(NUM_RECORDS):
        record = generate_record(i + 1)
        records.append(record)
        
        # Add some duplicates (about 1% of records)
        if random.random() < 0.01 and i > 0:
            duplicate_record = records[random.randint(0, len(records) - 1)].copy()
            duplicate_record['property_id'] = f"PROP_{NUM_RECORDS + duplicate_count + 1:08d}"
            records.append(duplicate_record)
            duplicate_count += 1
        
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1} records...")
    
    print(f"Writing {len(records)} records to {OUTPUT_FILE}...")
    
    # Write to CSV
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'property_id', 'address', 'city', 'location_code', 'sale_price',
            'sale_date', 'square_meters', 'property_type', 'seller_type'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(records)
    
    print(f"✓ Sample data generated: {OUTPUT_FILE}")
    print(f"  Total records: {len(records)}")
    print(f"  Including duplicates: {duplicate_count}")
    
    # Also create the reference data files
    create_reference_files()


def create_reference_files():
    """Create reference data files for validators."""
    print("\nCreating reference data files...")
    
    # Create valid locations file
    locations_file = './data/valid_locations.txt'
    with open(locations_file, 'w') as f:
        for location_code in VALID_LOCATION_CODES:
            f.write(location_code + '\n')
    print(f"✓ Created {locations_file}")
    
    # Create location mapping file
    import json
    mapping_file = './data/location_mapping.json'
    with open(mapping_file, 'w') as f:
        json.dump(LOCATION_CODE_MAP, f, indent=2)
    print(f"✓ Created {mapping_file}")


if __name__ == '__main__':
    main()
