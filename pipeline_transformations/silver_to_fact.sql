/*
================================================================================
Gold Layer: Fact Tables from Silver
================================================================================

Purpose:
    Creates fact tables in the gold layer by transforming silver data into
    analytics-ready fact tables for star schema data warehouse design.

Architecture Pattern:
    Silver (validated, cleaned) → Gold (fact tables for analytics)
    
    Medallion Layer: Gold (Business-Level Aggregates and Facts)
    
Data Flow:
    silver.claims_silver → STREAM → JOIN policy → gold.fact_claims
    silver.sales_silver  → STREAM → JOIN policy → gold.fact_sales

Key Features:
    - Streaming tables: Real-time updates as silver data arrives
    - Enrichment joins: Add dimension keys (customer_id) via policy lookup
    - Calculated metrics: Processing days, flags, totals derived from source data
    - Denormalized design: Includes both fact measures and dimensional attributes
    - WHERE filters: Ensures required fact IDs are present (no null keys)
    - Audit columns: Tracks when fact records were inserted

Design Principles:
    - Facts capture business events (claims filed, cars sold)
    - Measures are numeric, additive where possible (amounts, counts, days)
    - Dimension foreign keys enable joins to dimension tables
    - Calculated fields provide pre-computed analytics metrics
    - NULL handling ensures robust calculations (COALESCE, NULLIF)

Note:
    These fact tables serve as the core transactional data for analytics,
    combining with dimension tables (dim_customers, dim_policy, dim_cars)
    to enable comprehensive business intelligence reporting.
*/


-- ============================================================
-- FACT TABLE: Claims
-- ============================================================
-- Purpose: Insurance claims fact table for claims analytics
-- Source: silver.claims_silver (validated claims)
-- Enrichment: Joins with policy_silver to get customer_id
-- Grain: One record per claim (claim_id)
-- Measures: Claim amounts (injury, property, vehicle), processing time
-- ============================================================

CREATE OR REFRESH STREAMING TABLE bricksquad.gold.fact_claims
AS
SELECT
    -- ========================================
    -- FACT IDENTIFIERS & DIMENSION KEYS
    -- ========================================
    c.claim_id AS claim_id,                    -- Primary fact identifier (natural key)
    c.policy_id AS policy_number,              -- Foreign key to dim_policy
    p.customer_id AS customer_id,              -- Foreign key to dim_customers (enriched via policy join)
    
    -- ========================================
    -- INCIDENT ATTRIBUTES
    -- ========================================
    c.incident_date AS incident_date,          -- When incident occurred (time dimension)
    c.incident_state AS incident_state,        -- Where incident occurred (geography)
    c.incident_city AS incident_city,          -- City of incident
    c.incident_location AS incident_location,  -- Specific location description
    c.incident_type AS incident_type,          -- Type of incident (e.g., Vehicle Theft, Collision)
    c.collision_type AS collision_type,        -- Type of collision (e.g., Front, Rear, Side)
    c.incident_severity AS incident_severity,  -- Severity level (e.g., Minor, Major, Total Loss)
    
    -- ========================================
    -- INVESTIGATION DETAILS
    -- ========================================
    c.authorities_contacted AS authorities_contacted,          -- Which authorities responded (Police, Fire, etc.)
    c.number_of_vehicles_involved AS number_of_vehicles_involved,  -- Count of vehicles in incident
    c.property_damage AS property_damage,                      -- Property damage indicator (Yes/No)
    c.bodily_injuries AS bodily_injuries,                      -- Count of injured persons
    c.witnesses AS witnesses,                                  -- Count of witnesses
    c.police_report_available AS police_report_available,      -- Police report indicator (Yes/No)
    
    -- ========================================
    -- CLAIM AMOUNTS (MEASURES)
    -- ========================================
    c.injury AS injury_amount,                 -- Injury claim amount ($)
    c.property AS property_amount,             -- Property damage claim amount ($)
    c.vehicle AS vehicle_amount,               -- Vehicle damage claim amount ($)
    
    -- ========================================
    -- CLAIM STATUS & TIMESTAMPS
    -- ========================================
    c.claim_rejected AS claim_rejected,        -- Rejection status (Y/N)
    c.claim_logged_on AS claim_logged_on,      -- When claim was first reported
    c.claim_processed_on AS claim_processed_on, -- When claim was resolved/processed
    
    -- ========================================
    -- CALCULATED METRICS
    -- ========================================
    
    -- Processing time: Days from claim logged to claim processed
    -- NULLIF handles '-' placeholder values (converts to NULL before date parsing)
    DATEDIFF(
        TO_DATE(NULLIF(c.claim_processed_on, '-'), 'yyyy-MM-dd'),
        TO_DATE(NULLIF(c.claim_logged_on, '-'), 'yyyy-MM-dd')
    ) AS processing_days,
    
    -- Binary flag: 1 if claim rejected, 0 otherwise
    -- Handles case variations ('yes', 'YES', 'y', 'Y')
    CASE 
        WHEN LOWER(c.claim_rejected) IN ('yes', 'y') THEN 1
        ELSE 0
    END AS rejected_flag,
    
    -- Total claim amount: Sum of all claim components
    -- COALESCE handles NULL values (treats as 0 for summation)
    COALESCE(c.injury, 0) +
    COALESCE(c.property, 0) +
    COALESCE(c.vehicle, 0) AS total_claim_amount,
    
    -- ========================================
    -- AUDIT COLUMN
    -- ========================================
    current_timestamp() AS insert_timestamp        -- When record was inserted into fact table

FROM STREAM(bricksquad.silver.claims_silver) c  -- Streaming read: real-time claim updates
LEFT JOIN bricksquad.silver.policy_silver p     -- Batch read: enrich with customer_id from policy
    ON c.policy_id = p.policy_number            -- Join on policy identifier
WHERE c.claim_id IS NOT NULL;                   -- Data quality: filter out records with missing fact key


-- ============================================================
-- FACT TABLE: Sales
-- ============================================================
-- Purpose: Car sales fact table for sales analytics
-- Source: silver.sales_silver (validated sales transactions)
-- Enrichment: Joins with policy_silver to get customer_id (car owner)
-- Grain: One record per sale transaction (sales_id)
-- Measures: Selling price, days listed, unsold flag
-- ============================================================

CREATE OR REFRESH STREAMING TABLE bricksquad.gold.fact_sales
AS
SELECT
    -- ========================================
    -- FACT IDENTIFIERS & DIMENSION KEYS
    -- ========================================
    s.sales_id                           AS sales_id,        -- Primary fact identifier (natural key)
    s.car_id                             AS car_id,          -- Foreign key to dim_cars
    p.customer_id                        AS customer_id,     -- Foreign key to dim_customers (enriched via policy join)
    
    -- ========================================
    -- SALES TIMESTAMPS
    -- ========================================
    s.ad_placed_on AS ad_placed_on,                          -- When car listing was created (time dimension)
    s.sold_on     AS sold_on,                                -- When car was sold (NULL if still unsold)
    
    -- ========================================
    -- SALES MEASURES
    -- ========================================
    s.original_selling_price             AS original_selling_price,  -- Listed/sold price ($)
    
    -- ========================================
    -- SALES ATTRIBUTES
    -- ========================================
    s.region                             AS region,          -- Geographic region (West, Central, East, South)
    s.state                              AS state,           -- State where sale occurred
    s.city                               AS city,            -- City where sale occurred
    s.seller_type                        AS seller_type,     -- Seller type (Dealer, Individual)
    s.owner                              AS owner,           -- Ownership history (First Owner, Second Owner, etc.)
    
    -- ========================================
    -- CALCULATED METRICS
    -- ========================================
    
    -- Unsold flag: 1 if car hasn't sold yet, 0 if sold
    -- Checks for NULL or empty string in sold_on
    CASE
        WHEN s.sold_on IS NULL OR TRIM(s.sold_on) = '' THEN 1
        ELSE 0
    END                                  AS unsold_flag,
    
    -- Days listed: Time from ad placement to sale (or current date if unsold)
    -- For unsold cars: days from ad_placed_on to today
    -- For sold cars: days from ad_placed_on to sold_on
    CASE
        WHEN s.sold_on IS NULL OR TRIM(s.sold_on) = ''
            THEN DATEDIFF(CURRENT_DATE(), TO_DATE(s.ad_placed_on, 'yyyy-MM-dd'))
        ELSE DATEDIFF(TO_DATE(s.sold_on, 'yyyy-MM-dd'), TO_DATE(s.ad_placed_on, 'yyyy-MM-dd'))
    END                                  AS days_listed,
    
    -- ========================================
    -- AUDIT COLUMN
    -- ========================================
    CURRENT_TIMESTAMP()                  AS insert_timestamp      -- When record was inserted into fact table

FROM STREAM(bricksquad.silver.sales_silver) s  -- Streaming read: real-time sales updates
LEFT JOIN bricksquad.silver.policy_silver p    -- Batch read: enrich with customer_id (car owner)
    ON s.car_id = p.car_id                     -- Join on car identifier (policy holders are car owners)
WHERE s.sales_id IS NOT NULL;                  -- Data quality: filter out records with missing fact key
