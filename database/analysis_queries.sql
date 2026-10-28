-- ============================================================================
-- Real Estate Property Sales and Market Valuation
-- Business Intelligence & Verification SQL Queries
-- ============================================================================

-- ============================================================================
-- 1. VERIFICATION QUERY: Verify 2.5M+ Records Loaded & Data Integrity
-- ============================================================================
SELECT 
    COUNT(*) AS total_records_loaded,
    COUNT(DISTINCT postcode) AS unique_postcodes,
    COUNT(DISTINCT town_city) AS unique_cities_and_towns,
    MIN(sale_year) AS earliest_transaction_year,
    MAX(sale_year) AS latest_transaction_year,
    ROUND(AVG(price), 2) AS overall_avg_price,
    ROUND(AVG(data_quality_score), 1) AS overall_avg_quality_score,
    ROUND(SUM(price) / 1000000000.0, 2) AS total_market_volume_gbp_billions
FROM properties;


-- ============================================================================
-- 2. BUSINESS SCENARIO QUERY: Top 15 Cities by Transaction Volume & Average Valuation
-- Identifies the most active real estate markets and their average price points.
-- ============================================================================
SELECT 
    town_city,
    COUNT(*) AS transaction_count,
    ROUND(AVG(price), 2) AS avg_price,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::NUMERIC, 2) AS median_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM properties
GROUP BY town_city
HAVING COUNT(*) >= 1000
ORDER BY transaction_count DESC
LIMIT 15;


-- ============================================================================
-- 3. BUSINESS SCENARIO QUERY: Year-over-Year (YoY) Price Appreciation by Property Type
-- Calculates annual growth rate across Detached, Semi-Detached, Terraced, and Flats.
-- ============================================================================
WITH annual_averages AS (
    SELECT 
        property_type_category,
        sale_year,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(*) AS sales_volume
    FROM properties
    GROUP BY property_type_category, sale_year
)
SELECT 
    property_type_category,
    sale_year,
    sales_volume,
    avg_price,
    LAG(avg_price, 1) OVER (
        PARTITION BY property_type_category 
        ORDER BY sale_year
    ) AS prior_year_avg_price,
    ROUND(
        (avg_price - LAG(avg_price, 1) OVER (PARTITION BY property_type_category ORDER BY sale_year)) 
        / NULLIF(LAG(avg_price, 1) OVER (PARTITION BY property_type_category ORDER BY sale_year), 0) * 100.0,
        2
    ) AS yoy_growth_percent
FROM annual_averages
ORDER BY property_type_category, sale_year;


-- ============================================================================
-- 4. BUSINESS SCENARIO QUERY: Regional Valuation Disparity and Market Concentration
-- Enables property developers to identify emerging growth corridors vs mature markets.
-- ============================================================================
SELECT 
    region,
    COUNT(*) AS total_sales,
    ROUND((COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM properties) * 100.0), 2) AS market_share_percent,
    ROUND(AVG(price), 2) AS mean_price,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::NUMERIC, 2) AS median_price,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price)::NUMERIC - 
          PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price)::NUMERIC, 2) AS interquartile_range
FROM properties
GROUP BY region
ORDER BY median_price DESC;


-- ============================================================================
-- 5. BUSINESS SCENARIO QUERY: Tenure Valuation Premium (Freehold vs. Leasehold)
-- Quantifies the price differential attributable to land ownership tenure.
-- ============================================================================
SELECT 
    property_type_category,
    duration_type,
    COUNT(*) AS count_transactions,
    ROUND(AVG(price), 2) AS avg_sale_price,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::NUMERIC, 2) AS median_sale_price
FROM properties
WHERE duration_type IN ('Freehold', 'Leasehold')
GROUP BY property_type_category, duration_type
ORDER BY property_type_category, duration_type;


-- ============================================================================
-- 6. DATA QUALITY AUDIT QUERY: Data Health by Region
-- Displays quality score breakdown and flagged outlier prevalence.
-- ============================================================================
SELECT 
    region,
    COUNT(*) AS total_properties,
    ROUND(AVG(data_quality_score), 2) AS avg_quality_score,
    SUM(CASE WHEN (metadata->>'is_outlier')::boolean = TRUE THEN 1 ELSE 0 END) AS outlier_count,
    ROUND(
        (SUM(CASE WHEN (metadata->>'is_outlier')::boolean = TRUE THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100.0), 
        3
    ) AS outlier_percentage
FROM properties
GROUP BY region
ORDER BY avg_quality_score DESC;
