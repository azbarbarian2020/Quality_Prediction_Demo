# STREAMING MACHINE PREDICTIVE QUALITY DEMO SETUP

## Streaming Demo Table Setup
1.  Create a database called demo_db
2.  Create a schema called streaming
3.  Create machine_tbl
4.  Create machine_stats view
5.  Create my_stage stage
6.  Create docs stage

## Run GENERATE_TRAINING_DATA Notebook
1.  Add numpy package
2.  Import packages and set session
3.  Generate Training Data to Pandas DataFrame
4.  Write dataframe to train_table

## Run YIELD_PREDICTION Notebook
1.  Add numpy, sckit-learn and pandas packages
2.  Import packages and set session
3.  Import train_table, set features and label.  Establish train/test split
4.  Create class, LinearRegression model, and write to  .pkl
5.  Save model.pkl to stage
6.  Create PREDICT_MODEL UDF
7.  Test UDF on train_table
8.  Run Evaluation Metrics

## Create Search Service mill_ss2
1.  Upload mill manuals to docs stage (had to break up into two smaller files)
2.  Create and load RAW_TEXT table from pdf files in docs stage using PARSE_DOCUMENT Cortex function
3.  Create and load CHUNKED_TEXT table from RAW_TEXT table using SPLIT_TEXT_RECURSIVE_CHARACTER Cortex function
4.  Create search service from CHUNKED_TEXT table
5.  Test Search Service

## Create and Run Streaming Simulator
1.  Create new Streamlit app in Demo_DB database and Streaming Schema
2.  Copy and paste Streaming_sim.py

## Create Cortex Analyst Service 
1.  Open Cortex Analyst in demo_db database and streaming schema
2.  Choose my_stage to store the semantic file
3.  Name file machine_performance
4.  Select the train_table and machine_tbl and all their columns
5.  Apply Custom instructions
6.  Add following synonyms to both occurrences of MACHINE_NAME & BATCH_ID
   *  MACHINE_NAME: Machine Name, Machine, Machine ID
   *  BATCH_ID: Batch ID, Batch, Run, Run ID
7.  Enter and test the following prompt:
   *  What measures most correlate towards a lower quality yield, ranked highest to lowest, per machine?
       *  If it looks correct, add it as a Verified query
       *  Name it What measures most affect quality yield per machine?
   *  Show me the current abnormal readings count per measure, per machine?
       *  If it looks correct, add it as a Verified query
       *  Name it Abnormal reading count per machine
8.  Make sure to save your semantic model

## Streaming Table Setup Part II
1.  Create view predictive_stats

## Create Dashboard
1.  Create new Streamlit app in Demo_DB database and Streaming Schema
2.  Copy and paste Streaming_Dashboard.py

