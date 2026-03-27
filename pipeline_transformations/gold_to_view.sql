/*
================================================================================
Gold Layer: Analytics Materialized Views
================================================================================

Purpose:
    Creates business analytics materialized views by aggregating and analyzing
    gold layer fact and dimension tables. These views provide pre-computed
    metrics for reporting dashboards and business intelligence.

Architecture Pattern:
    Gold (Facts + Dimensions) → Aggregation → Gold (Analytics Views)
    
    Medallion Layer: Gold Analytics (Business-Ready Reports)
    
Data Flow:
    fact_claims + dim_customers + dim_policy → claim_rejection_rate_by_region_policy
    fact_claims + dim_customers             → avg_claim_processing_time_by_region
    fact_sales + dim_cars                   → unsold_cars_by_model_region

Key Features:
    - Materialized views: Pre-computed aggregations for fast query performance
    - Star schema joins: Combine facts with dimensions for enriched analytics
    - Business metrics: KPIs like rejection rates, processing times, inventory aging
    - Aggregation levels: Region, policy type, car model (reporting dimensions)
    - NULL handling: COALESCE for missing dimension values, WHERE filters for facts

Design Principles:
    - Views are read-optimized for dashboards and reports
    - Aggregations are pre-computed to reduce query latency
    - Grain is carefully chosen for common reporting needs
    - Calculations use ROUND for readability and consistency
    - LEFT JOINs ensure all facts are included even if dimensions are missing

Use Cases:
    - Executive dashboards showing claim rejection trends
    - Operations reports on claim processing efficiency
    - Inventory management for unsold car listings
    - Regional performance analysis
    - Policy type profitability analysis
*/


-- ============================================================
-- ANALYTICS VIEW 1: Claim Rejection Rate by Region & Policy Type
-- ============================================================
-- Purpose: Analyze claim rejection patterns across regions and policy types
-- Source: fact_claims joined with dim_policy and dim_customers
-- Grain: One record per (customer state, policy CSL type) combination
-- Measures: Total claims, rejected claims, rejection rate percentage
-- Use Case: Identify problematic regions or policy types with high rejection rates
-- ============================================================

CREATE OR REPLACE MATERIALIZED VIEW bricksquad.gold.claim_rejection_rate_by_region_policy
AS
SELECT
    -- ========================================
    -- DIMENSIONAL ATTRIBUTES (GROUP BY)
    -- ========================================
    dc.State AS customer_region,           -- Customer's home state (dimension)
    dp.policy_csl AS policy_type,          -- Policy Combined Single Limit type (e.g., "100/300/100")
    
    -- ========================================
    -- AGGREGATE MEASURES
    -- ========================================
    COUNT(*) AS total_claims,              -- Total number of claims in this segment
    SUM(fc.rejected_flag) AS rejected_claims,  -- Count of rejected claims (rejected_flag = 1)
    
    -- Rejection rate calculation:
    -- (Rejected claims / Total claims) * 100
    -- Rounds to 2 decimal places for readability (e.g., 15.67%)
    ROUND((SUM(fc.rejected_flag) * 100.0) / COUNT(*), 2) AS rejection_rate_pct

FROM bricksquad.gold.fact_claims fc        -- Base fact table (claims events)
LEFT JOIN bricksquad.gold.dim_policy dp    -- Enrich with policy details
    ON fc.policy_number = dp.policy_number
LEFT JOIN bricksquad.gold.dim_customers dc -- Enrich with customer geography
    ON fc.customer_id = dc.CustomerID

-- Aggregation level: by customer state and policy type
GROUP BY
    dc.State,
    dp.policy_csl;


-- ============================================================
-- ANALYTICS VIEW 2: Average Claim Processing Time by Region
-- ============================================================
-- Purpose: Measure operational efficiency of claim processing by region
-- Source: fact_claims joined with dim_customers
-- Grain: One record per customer state
-- Measures: Average processing days (from claim logged to processed)
-- Use Case: Identify regions with slow claim processing for operational improvement
-- ============================================================

CREATE OR REPLACE MATERIALIZED VIEW bricksquad.gold.avg_claim_processing_time_by_region
AS
SELECT
    -- ========================================
    -- DIMENSIONAL ATTRIBUTES (GROUP BY)
    -- ========================================
    dc.State AS customer_region,           -- Customer's home state (dimension)
    
    -- ========================================
    -- AGGREGATE MEASURES
    -- ========================================
    
    -- Average processing time calculation:
    -- Mean of processing_days (already calculated in fact_claims as DATEDIFF)
    -- Rounds to 2 decimal places (e.g., 12.45 days)
    ROUND(AVG(fc.processing_days), 2) AS avg_processing_days

FROM bricksquad.gold.fact_claims fc        -- Base fact table (claims events)
LEFT JOIN bricksquad.gold.dim_customers dc -- Enrich with customer geography
    ON fc.customer_id = dc.CustomerID

-- Filter: Only include claims with valid processing time
-- Excludes claims that haven't been processed yet (processing_days = NULL)
WHERE fc.processing_days IS NOT NULL

-- Aggregation level: by customer state
GROUP BY
    dc.State;


-- ============================================================
-- ANALYTICS VIEW 3: Unsold Cars by Model & Region
-- ============================================================
-- Purpose: Track inventory aging for unsold cars by model and region
-- Source: fact_sales joined with dim_cars
-- Grain: One record per (sales state, car name, car model) combination
-- Measures: Count of unsold cars, average days listed, max days listed
-- Use Case: Identify slow-moving inventory for pricing adjustments or promotions
-- ============================================================

CREATE OR REPLACE MATERIALIZED VIEW bricksquad.gold.unsold_cars_by_model_region
AS
SELECT
    -- ========================================
    -- DIMENSIONAL ATTRIBUTES (GROUP BY)
    -- ========================================
    
    -- Handle NULL state values: Replace with 'Unknown' for reporting clarity
    COALESCE(fs.state, 'Unknown') AS customer_region,
    
    car.name AS car_name,                  -- Car make/name (e.g., "Toyota")
    car.model AS car_model,                -- Car model (e.g., "Camry 2018")
    
    -- ========================================
    -- AGGREGATE MEASURES
    -- ========================================
    COUNT(*) AS unsold_cars_count,         -- Total number of unsold cars in this segment
    
    -- Average inventory aging:
    -- Mean days that cars have been listed without selling
    -- Rounds to 2 decimal places (e.g., 45.67 days)
    ROUND(AVG(fs.days_listed), 2) AS avg_days_unsold,
    
    -- Maximum inventory aging:
    -- Longest listing duration for any car in this segment
    -- Helps identify extreme outliers that may need intervention
    MAX(fs.days_listed) AS max_days_unsold

FROM bricksquad.gold.fact_sales fs         -- Base fact table (sales transactions/listings)
LEFT JOIN bricksquad.gold.dim_cars car     -- Enrich with car details
    ON fs.car_id = car.car_id

-- Filter: Only include unsold cars (unsold_flag = 1)
-- Excludes completed sales transactions
WHERE fs.unsold_flag = 1

-- Aggregation level: by state, car name, and model
-- COALESCE in GROUP BY matches the SELECT clause
GROUP BY
    COALESCE(fs.state, 'Unknown'),
    car.name,
    car.model;
