-- ============================================================================
-- Nationwide Real Estate Property Assessment and Historical Sales Database
-- PostgreSQL Schema Definition and Performance Tuning
-- Target Dataset Size: >= 2,500,000 Records
-- ============================================================================

-- Create Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ----------------------------------------------------------------------------
-- Dimension Table 1: Property Types (Normalized Classification)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_property_types (
    property_type_code VARCHAR(10) PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dim_property_types (property_type_code, category_name, description)
VALUES 
    ('D', 'Detached', 'Detached single-family residential house'),
    ('S', 'Semi-Detached', 'Semi-detached residential structure'),
    ('T', 'Terraced', 'Terraced row house structure'),
    ('F', 'Flat/Maisonette', 'Apartment, flat, or maisonette unit'),
    ('O', 'Other', 'Commercial, industrial, land, or other property category')
ON CONFLICT (property_type_code) DO NOTHING;

-- ----------------------------------------------------------------------------
-- Main Enterprise Table: properties (Historical Sales Transactions)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS properties (
    transaction_id VARCHAR(100) PRIMARY KEY,
    price NUMERIC(15, 2) NOT NULL CHECK (price > 0),
    date_of_transfer DATE NOT NULL,
    sale_year INT NOT NULL,
    sale_month INT NOT NULL,
    months_since_transaction INT,
    postcode VARCHAR(20) NOT NULL,
    town_city VARCHAR(150) NOT NULL,
    district VARCHAR(150),
    county VARCHAR(150),
    region VARCHAR(100),
    country VARCHAR(100),
    property_type VARCHAR(10) NOT NULL REFERENCES dim_property_types(property_type_code),
    property_type_category VARCHAR(50) NOT NULL,
    duration_type VARCHAR(30),
    is_freehold BOOLEAN NOT NULL DEFAULT TRUE,
    is_new_build BOOLEAN NOT NULL DEFAULT FALSE,
    address_line VARCHAR(500),
    square_meters NUMERIC(10, 2),
    price_per_square_meter NUMERIC(12, 2),
    latitude NUMERIC(10, 7),
    longitude NUMERIC(10, 7),
    data_quality_score SMALLINT NOT NULL CHECK (data_quality_score BETWEEN 0 AND 100),
    dedup_hash VARCHAR(64) NOT NULL UNIQUE,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- High-Performance Indexes for 2.5M+ Record Querying
-- ----------------------------------------------------------------------------
-- Geographic and Postcode Lookups
CREATE INDEX IF NOT EXISTS idx_properties_postcode ON properties(postcode);
CREATE INDEX IF NOT EXISTS idx_properties_town_city ON properties(town_city);
CREATE INDEX IF NOT EXISTS idx_properties_district ON properties(district);
CREATE INDEX IF NOT EXISTS idx_properties_region ON properties(region);

-- Temporal and Financial Trend Queries
CREATE INDEX IF NOT EXISTS idx_properties_date_of_transfer ON properties(date_of_transfer);
CREATE INDEX IF NOT EXISTS idx_properties_price ON properties(price);
CREATE INDEX IF NOT EXISTS idx_properties_category ON properties(property_type_category);

-- Composite Indexes for Advanced Aggregations
CREATE INDEX IF NOT EXISTS idx_properties_city_year ON properties(town_city, sale_year);
CREATE INDEX IF NOT EXISTS idx_properties_region_year_price ON properties(region, sale_year, price);

-- ----------------------------------------------------------------------------
-- Analytical Views for Business Intelligence Reporting
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW view_regional_market_trends AS
SELECT 
    region,
    sale_year,
    COUNT(*) AS total_transactions,
    ROUND(AVG(price), 2) AS average_sale_price,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::NUMERIC, 2) AS median_sale_price,
    MIN(price) AS min_sale_price,
    MAX(price) AS max_sale_price,
    ROUND(AVG(data_quality_score), 1) AS avg_data_quality_score
FROM properties
GROUP BY region, sale_year
ORDER BY region, sale_year;
