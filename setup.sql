create database if not exists demo_db;
create schema if not exists streaming;

create stage if not exists my_stage
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

create stage if not exists docs
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

--------Main table used to stream data into

Create table if not exists machine_tbl (
    Machine_Name string,
    Batch string,
    TIMESTAMP TIMESTAMP_NTZ(9),
    Spindle_Speed Integer,
    Feed_Rate Integer,
    Vibration Float,
    Tool_Wear Float
);

--Query Table
select * from machine_tbl order by 3 desc;
--Reset for New demo
truncate table machine_tbl;


---------Aggergate stats from machine_tbl

CREATE view if not exists MACHINE_STATS AS 
select MACHINE_NAME, 
  ((SUM(CASE
    WHEN Spindle_Speed>3200THEN 1
    WHEN SPindle_Speed <2800 THEN 1
    ELSE 0
    END) / count(Spindle_Speed))*100) as spindle_speed_abnormal_pct,
avg(Spindle_Speed) AS spindle_speed_avg, 
 ((SUM(CASE
    WHEN Vibration>.3 THEN 1
    WHEN Vibration <.1 THEN 1
    ELSE 0
    END) / count(Vibration))*100) as vibration_abnormal_pct,
  avg(Vibration) AS vibration_avg, 
  ((SUM(CASE
    WHEN FEED_RATE>120 THEN 1
    WHEN FEED_RATE <100 THEN 1
    ELSE 0
    END) / count(FEED_RATE))*100) as feed_rate_abnormal_pct,
  AVG(FEED_RATE) AS feed_rate_avg, 
  max(TOOL_WEAR) as tool_wear
  from machine_tbl group by 1;


----------Apply PREDICT_MODEL to get real time predictions **After creating train_table and PREDICT_MODEL UDF

create view if not exists predictive_stats as (
SELECT 
    MACHINE_NAME,
    FEED_RATE_ABNORMAL_PCT,
    FEED_RATE_AVG,
    VIBRATION_ABNORMAL_PCT, 
    VIBRATION_AVG, 
    SPINDLE_SPEED_ABNORMAL_PCT,
    SPINDLE_SPEED_AVG,
    TOOL_WEAR,
    PREDICT_MODEL(
        ARRAY_CONSTRUCT(
            FEED_RATE_ABNORMAL_PCT,
            FEED_RATE_AVG,
            VIBRATION_ABNORMAL_PCT, 
            VIBRATION_AVG, 
            SPINDLE_SPEED_ABNORMAL_PCT,
            SPINDLE_SPEED_AVG,
            TOOL_WEAR
        )
    ) AS PREDICTED_YIELD
FROM MACHINE_STATS);
