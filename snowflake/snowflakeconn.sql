-- storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::331550282864:role/snowflake-s3-access-role'
STORAGE_ALLOWED_LOCATIONS = (
    's3://sophia-real-time-pipeline-123/models/stock-model/scored_data/'
);

-- values
DESC INTEGRATION s3_integration;


-- file formatting
CREATE OR REPLACE FILE FORMAT my_parquet_format
TYPE = PARQUET;


-- create stage
CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://sophia-real-time-pipeline-123/models/stock-model/scored_data/'
STORAGE_INTEGRATION = s3_integration
FILE_FORMAT = my_parquet_format;


-- create table
CREATE OR REPLACE TABLE my_table USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@my_s3_stage',
        FILE_FORMAT => 'my_parquet_format'
    ))
);

-- put data in table
COPY INTO my_table
FROM @my_s3_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';


-- verify data
SELECT * FROM my_table LIMIT 50;

-- test
LIST @my_s3_stage;
