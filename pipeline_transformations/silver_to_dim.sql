/*
================================================================================
Gold Layer: Dimension Tables from Silver
================================================================================

Purpose:
    Creates dimension tables in the gold layer by promoting cleaned silver data
    into analytics-ready dimension tables for star schema data warehouse design.

Architecture Pattern:
    Silver (validated, cleaned) → Gold (dimension tables for analytics)
    
    Medallion Layer: Gold (Business-Level Aggregates and Dimensions)
    
Data Flow:
    silver.customers_silver → STREAM → gold.dim_customers
    silver.policy_silver    → STREAM → gold.dim_policy
    silver.cars_silver      → STREAM → gold.dim_cars

Key Features:
    - Streaming tables: Real-time updates as silver data arrives
    - SELECT DISTINCT: Ensures dimensional uniqueness
    - WHERE filters: Guarantees required IDs are present (no null keys)
    - Audit columns: Tracks when dimension records were inserted
    - Read-optimized: Designed for JOIN operations in fact tables

Design Principles:
    - Dimensions are slowly-changing: Updates flow through as new records
    - No SCD Type 2 history: Latest values overwrite (SCD Type 1)
    - Minimal transformation: Direct pass-through from validated silver
    - Referential integrity: Null keys filtered to ensure clean joins

Note:
    These dimension tables serve as the foundational reference data for
    fact tables (e.g., fact_claims, fact_sales) in the gold layer.
*/


-- ============================================================
-- DIMENSION TABLE: Customers
-- ============================================================
-- Purpose: Customer master dimension for analytics
-- Source: silver.customers_silver (validated customer data)
-- Usage: Join with fact tables using CustomerID
-- SCD Type: Type 1 (latest values only, no history)
-- ============================================================

CREATE OR REFRESH STREAMING TABLE bricksquad.gold.dim_customers
COMMENT "Staging customer dimension from silver with insert/update audit columns"
AS
SELECT DISTINCT  -- Ensures one record per CustomerID (dimensional uniqueness)
    -- Primary key (natural key)
    CustomerID,
    
    -- Geographic attributes
    Region,        -- West, Central, East, South
    State,         -- State name (cleaned)
    City,          -- City name (cleaned)
    
    -- Demographic attributes
    Job,           -- Job title/category
    Marital,       -- Marital status (Single, Married, Divorced)
    Education,     -- Education level (Primary, Secondary, Tertiary)
    
    -- Financial/Risk attributes
    Default,       -- Default flag (0=No, 1=Yes)
    Balance,       -- Account balance
    HHInsurance,   -- Household insurance count
    CarLoan,       -- Car loan flag (0=No, 1=Yes)
    
    -- Audit column
    current_timestamp() AS insert_timestamp  -- When record was added to dimension
    
FROM STREAM(bricksquad.silver.customers_silver)  -- Streaming read: real-time updates
WHERE CustomerID IS NOT NULL;  -- Data quality: filter out records with missing primary key

-- ============================================================
-- DIMENSION TABLE: Policy
-- ============================================================
-- Purpose: Policy master dimension for analytics
-- Source: silver.policy_silver (validated policy data)
-- Usage: Join with fact tables using policy_number
-- SCD Type: Type 1 (latest values only, no history)
-- ============================================================

CREATE OR REFRESH STREAMING TABLE bricksquad.gold.dim_policy
COMMENT "Policy dimension built from silver.policy with audit columns"
AS
SELECT DISTINCT  -- Ensures one record per policy_number (dimensional uniqueness)
    -- Primary key (natural key)
    policy_number,
    
    -- Policy details
    policy_bind_date,          -- When policy was activated
    policy_state,              -- State where policy is effective
    policy_csl,                -- Combined Single Limit (e.g., "100/300/100")
    policy_deductable,         -- Deductible amount
    policy_annual_premium,     -- Annual premium amount
    umbrella_limit,            -- Umbrella coverage limit
    
    -- Foreign keys (for snowflake schema or validation)
    car_id,                    -- Links to dim_cars
    customer_id,               -- Links to dim_customers
    
    -- Audit column
    current_timestamp() AS insert_timestamp  -- When record was added to dimension
    
FROM STREAM(bricksquad.silver.policy_silver)  -- Streaming read: real-time updates
WHERE policy_number IS NOT NULL;  -- Data quality: filter out records with missing primary key

-- ============================================================
-- DIMENSION TABLE: Cars
-- ============================================================
-- Purpose: Car master dimension for analytics
-- Source: silver.cars_silver (validated car inventory data)
-- Usage: Join with fact tables using car_id
-- SCD Type: Type 1 (latest values only, no history)
-- ============================================================

CREATE OR REFRESH STREAMING TABLE bricksquad.gold.dim_cars
COMMENT "Car dimension built from silver.cars with audit columns"
AS
SELECT DISTINCT  -- Ensures one record per car_id (dimensional uniqueness)
    -- Primary key (natural key)
    car_id,
    
    -- Car identification
    name,          -- Car name/make
    model,         -- Car model
    
    -- Usage details
    km_driven,     -- Kilometers driven (odometer reading)
    
    -- Engine specifications
    fuel,          -- Fuel type (Petrol, Diesel, CNG, LPG)
    transmission,  -- Transmission type (Manual, Automatic)
    mileage,       -- Fuel efficiency (kmpl)
    engine,        -- Engine displacement (CC)
    max_power,     -- Maximum power (bhp)
    torque,        -- Torque specification
    
    -- Capacity
    seats,         -- Number of seats
    
    -- Audit column
    current_timestamp() AS insert_timestamp  -- When record was added to dimension
    
FROM STREAM(bricksquad.silver.cars_silver)  -- Streaming read: real-time updates
WHERE car_id IS NOT NULL;  -- Data quality: filter out records with missing primary key
