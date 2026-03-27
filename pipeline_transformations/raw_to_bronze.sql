-- ============================================================
-- BRONZE LAYER: Raw Ingestion via Auto Loader (cloud_files)
-- ============================================================


-- ============================================================
-- TABLE: customers
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bricksquad.bronze.customers_bronze
COMMENT "Raw customers data loaded exactly as received from all matching customer CSV files"
AS
SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _load_ts
FROM STREAM cloud_files(
  '/Volumes/bricksquad/bronze/raw_datafiles/autoinsurancedata/',
  'csv',
  map(
    'header', 'true',
    'inferSchema', 'false',
    'sep', ',',
    'cloudFiles.schemaEvolutionMode', 'addNewColumns',
    'recursiveFileLookup', 'true',
    'rescuedDataColumn', '_rescued_data',
    'pathGlobFilter', '[cC][uU][sS][tT][oO][mM][eE][rR][sS]*.csv'
  )
)
WHERE lower(_metadata.file_path) LIKE '%customers%'
;


-- ============================================================
-- TABLE: sales
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bricksquad.bronze.sales_bronze
COMMENT "Raw sales data loaded exactly as received from all matching sales CSV files"
AS
SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _load_ts
FROM STREAM cloud_files(
  '/Volumes/bricksquad/bronze/raw_datafiles/autoinsurancedata/',
  'csv',
  map(
    'header', 'true',
    'inferSchema', 'false',
    'sep', ',',
    'cloudFiles.schemaEvolutionMode', 'addNewColumns',
    'recursiveFileLookup', 'true',
    'rescuedDataColumn', '_rescued_data',
    'pathGlobFilter', '[sS][aA][lL][eE][sS]*.csv'
  )
)
WHERE lower(_metadata.file_path) LIKE '%sales%'
;


-- ============================================================
-- TABLE: claims
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bricksquad.bronze.claims_bronze
COMMENT "Raw claims data loaded exactly as received from all matching claims JSON files"
AS
SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _load_ts
FROM STREAM cloud_files(
  '/Volumes/bricksquad/bronze/raw_datafiles/autoinsurancedata/',
  'json',
  map(
    'multiLine', 'true',
    'cloudFiles.schemaEvolutionMode', 'addNewColumns',
    'recursiveFileLookup', 'true',
    'rescuedDataColumn', '_rescued_data',
    'pathGlobFilter', '[cC][lL][aA][iI][mM][sS]*.json'
  )
)
WHERE lower(_metadata.file_path) LIKE '%claims%'
;


-- ============================================================
-- TABLE: policy
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bricksquad.bronze.policy_bronze
COMMENT "Raw policy data loaded exactly as received from all matching policy CSV files"
AS
SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _load_ts
FROM STREAM cloud_files(
  '/Volumes/bricksquad/bronze/raw_datafiles/autoinsurancedata/',
  'csv',
  map(
    'header', 'true',
    'inferSchema', 'false',
    'sep', ',',
    'cloudFiles.schemaEvolutionMode', 'addNewColumns',
    'recursiveFileLookup', 'true',
    'rescuedDataColumn', '_rescued_data',
    'pathGlobFilter', '[pP][oO][lL][iI][cC][yY]*.csv'
  )
)
WHERE lower(_metadata.file_path) LIKE '%policy%'
;


-- ============================================================
-- TABLE: cars
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bricksquad.bronze.cars_bronze
COMMENT "Raw cars catalog data loaded exactly as received from all matching cars CSV files"
AS
SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _load_ts
FROM STREAM cloud_files(
  '/Volumes/bricksquad/bronze/raw_datafiles/autoinsurancedata/',
  'csv',
  map(
    'header', 'true',
    'inferSchema', 'false',
    'sep', ',',
    'cloudFiles.schemaEvolutionMode', 'addNewColumns',
    'recursiveFileLookup', 'true',
    'rescuedDataColumn', '_rescued_data',
    'pathGlobFilter', '[cC][aA][rR][sS]*.csv'
  )
)
WHERE lower(_metadata.file_path) LIKE '%cars%'
;