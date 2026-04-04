
-- =========================================
-- 1. STORAGE INTEGRATION (S3 CONNECTION)
-- =========================================
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::331550282864:role/snowflake-s3-access-role'
STORAGE_ALLOWED_LOCATIONS = (
    's3://sophia-real-time-pipeline-123/models/stock-model/scored_data/'
);

-- =========================================
-- 2. GET VALUES (RUN THIS ONCE)
-- =========================================
DESC INTEGRATION s3_integration;


-- =========================================
-- 3. FILE FORMAT (PARQUET)
-- =========================================
CREATE OR REPLACE FILE FORMAT my_parquet_format
TYPE = PARQUET;


-- =========================================
-- 4. CREATE STAGE (POINT TO YOUR S3 PATH)
-- =========================================
CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://sophia-real-time-pipeline-123/models/stock-model/scored_data/'
STORAGE_INTEGRATION = s3_integration
FILE_FORMAT = my_parquet_format;


-- =========================================
-- 5. CREATE TABLE
-- NOTE: Snowflake can auto-infer schema from Parquet
-- You can also define columns explicitly if you want
-- =========================================
CREATE OR REPLACE TABLE my_table USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage',
        FILE_FORMAT => 'my_parquet_format'
    ))
);


-- =========================================
-- 6. COPY DATA INTO TABLE
-- =========================================
COPY INTO my_table
FROM @my_s3_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';


-- =========================================
-- 7. VERIFY DATA
-- =========================================
SELECT * FROM my_table LIMIT 50;

-- =========================================
-- 8. TEST STAGE ACCESS (IMPORTANT DEBUG STEP)
-- =========================================
LIST @my_s3_stage;
