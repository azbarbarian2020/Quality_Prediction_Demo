STREAMING MACHINE PREDICTIVE QUALITY DEMO SETUP

Streaming Demo Table Setup
•	Create a database called demo_db
•	Create a schema called streaming
o	Create machine_tbl
o	Create machine_stats view
o	Create my_stage stage
o	Create docs stage

Run GENERATE_TRAINING_DATA Notebook
•	Add numpy package
•	Import packages and set session
•	Generate Training Data to Pandas DataFrame
•	Write dataframe to train_table

Run YIELD_PREDICTION Notebook
•	Add numpy, sckit-learn and pandas packages
•	Import packages and set session
•	Import train_table, set features and label.  Establish train/test split
•	Create class, LinearRegression model, and write to  .pkl
•	Save model.pkl to stage
•	Create PREDICT_MODEL UDF
•	Test UDF on train_table
•	Run Evaluation Metrics

Create Search Service mill_ss2
•	Upload mill manuals to docs stage (had to break up into two smaller files)
•	Create and load RAW_TEXT table from pdf files in docs stage using PARSE_DOCUMENT Cortex function
•	Create and load CHUNKED_TEXT table from RAW_TEXT table using SPLIT_TEXT_RECURSIVE_CHARACTER Cortex function
•	Create search service from CHUNKED_TEXT table
•	Test Search Service

Create and Run Streaming Simulator
•	Create new Streamlit app in Demo_DB database and Streaming Schema
•	Copy and paste Streaming_sim.py

Create Cortex Analyst Service 
•	Open Cortex Analyst in demo_db database and streaming schema
•	Choose my_stage to store the semantic file
•	Name file machine_performance
•	Select the train_table and machine_tbl and all their columns
•	Apply Custom instructions
•	Add following synonyms to both occurrences of MACHINE_NAME & BATCH_ID
o	  MACHINE_NAME: Machine Name, Machine, Machine ID
o	  BATCH_ID: Batch ID, Batch, Run, Run ID
•	Enter and test the following prompt:
o	What measures most correlate towards a lower quality yield, ranked highest to lowest, per machine?
	   If it looks correct, add it as a Verified query
	  Name it What measures most affect quality yield per machine?
o	Show me the current abnormal readings count per measure, per machine?
	  If it looks correct, add it as a Verified query
	    Name it Abnormal reading count per machine
•	Make sure to save your semantic model

Streaming Table Setup Part II
•	Create view predictive_stats

Create Dashboard
•	Create new Streamlit app in Demo_DB database and Streaming Schema
•	Copy and paste Streaming_Dashboard.py

![image](https://github.com/user-attachments/assets/4f7f6b3c-6d84-49af-a958-95eac7d1b15d)
