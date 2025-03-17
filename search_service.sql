use database demo_db;
use schema streaming;

-----Load mill_ngc_manual2023_part1.pdf and mill_ngc_manual2023_part2.pdf into docs stage

select * from DIRECTORY ('@docs')

-----Extract pdf text into RAW_TEXT table

CREATE OR REPLACE TABLE RAW_TEXT AS
SELECT 
    RELATIVE_PATH,
    TO_VARCHAR (
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT (
            '@docs',
            RELATIVE_PATH,
            {'mode': 'LAYOUT'} ):content
        ) AS EXTRACTED_LAYOUT 
FROM 
    DIRECTORY('@docs');

SELECT * FROM RAW_TEXT;

-- Create chunks from extracted content
CREATE OR REPLACE TABLE CHUNKED_TEXT AS
SELECT
   RELATIVE_PATH,
   c.INDEX::INTEGER AS CHUNK_INDEX,
   c.value::TEXT AS CHUNK_TEXT
FROM
   RAW_TEXT,
   LATERAL FLATTEN( input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (
      EXTRACTED_LAYOUT,
      'markdown',
      4000,
      400,
      ['\n\n', '\n', ' ', '']
   )) c;

SELECT * FROM CHUNKED_TEXT;

-- Create a Cortex Search Service for Mill Machine Doc
CREATE OR REPLACE CORTEX SEARCH SERVICE mill_ss2
  ON CHUNK_TEXT
  ATTRIBUTES RELATIVE_PATH
  WAREHOUSE = DEMO_WH
  TARGET_LAG = '1 hour'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS (
  SELECT
      CHUNK_TEXT,
      RELATIVE_PATH,
      CHUNK_INDEX
  FROM CHUNKED_TEXT
); 

---------Quick test of search service

SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'demo_db.test.mill_ss2',
      '{
        "query": "Tool Replacement",
        "columns":[
            "chunk_text"
        ],
        "limit":1
      }'
  )
)['results'] as results;