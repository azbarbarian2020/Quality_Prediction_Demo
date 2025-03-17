import streamlit as st
import snowflake.snowpark.context
from datetime import datetime, timedelta, date, time
import time as time_module
import pandas as pd
import random
import json

# Initialize session state variables
if 'running' not in st.session_state:
    st.session_state['running'] = False
if 'total_rows_generated' not in st.session_state:
    st.session_state['total_rows_generated'] = 0
if 'current_session_rows' not in st.session_state:
    st.session_state['current_session_rows'] = 0
if 'last_batch_time' not in st.session_state:
    st.session_state['last_batch_time'] = time_module.time()
if 'config' not in st.session_state:
    st.session_state['config'] = {}
if 'current_measure_values' not in st.session_state:
    st.session_state['current_measure_values'] = {}
if 'selected_table_info' not in st.session_state:
    st.session_state['selected_table_info'] = {
        'db': None,
        'schema': None,
        'table': None,
        'columns': None,
        'column_types': {}
    }
if 'current_timestamp' not in st.session_state:
    st.session_state['current_timestamp'] = None
if 'machine_configs' not in st.session_state:
    st.session_state['machine_configs'] = {}
if 'show_confirm_button' not in st.session_state:
    st.session_state['show_confirm_button'] = False
if 'settings_to_load' not in st.session_state:
    st.session_state['settings_to_load'] = None
if 'selected_hour' not in st.session_state:
    st.session_state['selected_hour'] = datetime.now().hour
if 'selected_minute' not in st.session_state:
    st.session_state['selected_minute'] = datetime.now().minute
if 'selected_date' not in st.session_state:
    st.session_state['selected_date'] = datetime.now().date()

def get_databases(session):
    return [row['name'] for row in session.sql("SHOW DATABASES").collect()]

def get_schemas(session, database):
    return [row['name'] for row in session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()]

def get_tables(session, database, schema):
    return [row['name'] for row in session.sql(f"SHOW TABLES IN {database}.{schema}").collect()]

def get_columns_with_types(session, database, schema, table):
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM {database}.INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{schema}' 
    AND TABLE_NAME = '{table}'
    ORDER BY ORDINAL_POSITION
    """
    results = session.sql(query).collect()
    return [(row['COLUMN_NAME'], row['DATA_TYPE']) for row in results]

def is_timestamp_type(data_type):
    timestamp_types = [
        'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ',
        'TIMESTAMP', 'DATETIME', 'DATE', 'TIME'
    ]
    return any(ts_type in data_type.upper() for ts_type in timestamp_types)

def format_timestamp_for_snowflake(timestamp_value, data_type):
    if timestamp_value is None:
        return None
    
    data_type = data_type.upper()
    if 'TIME' in data_type and 'TIMESTAMP' not in data_type and 'DATETIME' not in data_type:
        return timestamp_value.time()
    elif 'DATE' in data_type and 'TIME' not in data_type:
        return timestamp_value.date()
    else:
        return timestamp_value

def generate_measure_value(settings, data_type, current_value=None):
    if settings['mode'] == 'random':
        outside_nominal = random.uniform(0, 100) < settings['percent_outside']
        
        if outside_nominal:
            can_go_below = settings['total_min'] < settings['nominal_min']
            can_go_above = settings['total_max'] > settings['nominal_max']
            
            if can_go_above and not can_go_below:
                value = random.uniform(settings['nominal_max'], settings['total_max'])
            elif can_go_below and not can_go_above:
                value = random.uniform(settings['total_min'], settings['nominal_min'])
            elif can_go_above and can_go_below:
                if random.choice([True, False]):
                    value = random.uniform(settings['total_min'], settings['nominal_min'])
                else:
                    value = random.uniform(settings['nominal_max'], settings['total_max'])
            else:
                value = random.uniform(settings['nominal_min'], settings['nominal_max'])
        else:
            value = random.uniform(settings['nominal_min'], settings['nominal_max'])
        
        if 'INT' in data_type.upper():
            value = int(value)
            
        return value
            
    elif settings['mode'] == 'additive':
        if current_value is None:
            value = settings['initial_value']
        else:
            value = current_value + settings['increment']
            value = min(value, settings['max_value'])
        
        if 'INT' in data_type.upper():
            value = int(value)
            
        return value

def write_to_snowflake(session, df, database, schema, table):
    try:
        st.write(f"Writing to {database}.{schema}.{table}")
        
        preview_df = df.copy()
        column_types = st.session_state['selected_table_info']['column_types']
        
        for col in preview_df.columns:
            if col in column_types:
                data_type = column_types[col].upper()
                
                if any(int_type in data_type for int_type in ['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT']):
                    preview_df[col] = preview_df[col].astype('Int64')
                    preview_df[col] = preview_df[col].apply(lambda x: f"{int(x)}" if pd.notnull(x) else None)
                
                elif 'DECIMAL' in data_type or 'NUMERIC' in data_type:
                    if '(' in data_type:
                        scale = int(data_type.split(',')[1].split(')')[0])
                        preview_df[col] = preview_df[col].round(scale)
                
                elif 'FLOAT' in data_type or 'REAL' in data_type or 'DOUBLE' in data_type:
                    preview_df[col] = preview_df[col].round(6)
                
                elif any(ts_type in data_type for ts_type in ['TIMESTAMP', 'DATETIME', 'DATE', 'TIME']):
                    if preview_df[col].dtype == 'datetime64[ns]':
                        preview_df[col] = preview_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        st.write("Preview of data being written (matching table data types):")
        st.write(preview_df.head())
        
        timestamp_columns = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in timestamp_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        snowpark_df = session.create_dataframe(df)
        table_name = f"{database}.{schema}.{table}"
        snowpark_df.write.mode("append").save_as_table(table_name)
        st.write("Write successful")
        return True
    except Exception as e:
        st.error(f"Error writing to Snowflake: {str(e)}")
        st.error(f"Error details: {str(type(e))}")
        return False

def save_settings():
    try:
        session = snowflake.snowpark.context.get_active_session()
        
        # Get current database and schema from the connection
        current_db_schema = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
        settings_db = current_db_schema[0]
        settings_schema = current_db_schema[1]
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {settings_db}.{settings_schema}.SETTINGS_TABLE (
            NAME VARCHAR(16777216),
            DATA VARIANT,
            CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
        )
        """
        session.sql(create_sql).collect()

        settings_name = st.text_input("Settings Name", "Default")
        if st.button("Save Current Settings"):
            try:
                # Convert datetime objects to strings for JSON serialization
                config_copy = dict(st.session_state['config'])
                if 'current_timestamp' in config_copy and config_copy['current_timestamp']:
                    config_copy['current_timestamp'] = config_copy['current_timestamp'].isoformat()

                # Add special columns configuration
                special_columns = {
                    'machine_name_column': st.session_state['config'].get('machine_name_column'),
                    'batch_id_column': st.session_state['config'].get('batch_id_column'),
                    'timestamp_column': st.session_state['config'].get('timestamp_column'),
                    'timestamp_mode': st.session_state['config'].get('timestamp_mode')
                }

                settings_data = {
                    'table_info': st.session_state['selected_table_info'],
                    'machine_configs': st.session_state['machine_configs'],
                    'config': config_copy,
                    'special_columns': special_columns
                }
                
                insert_sql = f"""
                INSERT INTO {settings_db}.{settings_schema}.SETTINGS_TABLE (NAME, DATA)
                SELECT '{settings_name}', TO_VARIANT(parse_json('{json.dumps(settings_data).replace("'", "''")}'))
                """
                
                session.sql(insert_sql).collect()
                
                verify_sql = f"""
                SELECT COUNT(*) as cnt FROM {settings_db}.{settings_schema}.SETTINGS_TABLE 
                WHERE NAME = '{settings_name}'
                """
                result = session.sql(verify_sql).collect()
                
                if result and result[0]['CNT'] > 0:
                    st.success(f"Settings successfully saved as: {settings_name}")
                else:
                    st.warning("Settings may not have been saved properly. Please verify.")
                
            except Exception as e:
                st.error(f"Error executing SQL: {str(e)}")
                raise e

    except Exception as e:
        st.error(f"Error in save_settings: {str(e)}")
        st.error(f"Error type: {type(e)}")
        if hasattr(e, 'message'):
            st.error(f"Error message: {e.message}")

def load_saved_settings():
    try:
        session = snowflake.snowpark.context.get_active_session()
        
        # Get current database and schema from the connection
        current_db_schema = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
        settings_db = current_db_schema[0]
        settings_schema = current_db_schema[1]
        
        # Create the settings table if it doesn't exist
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {settings_db}.{settings_schema}.SETTINGS_TABLE (
            NAME VARCHAR(16777216),
            DATA VARIANT,
            CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
        )
        """
        try:
            session.sql(create_sql).collect()
        except Exception as e:
            st.error(f"Error creating settings table: {str(e)}")
            return
        
        query_sql = f"""
        SELECT NAME, TO_JSON(DATA) as DATA_JSON, CREATED_AT 
        FROM {settings_db}.{settings_schema}.SETTINGS_TABLE 
        ORDER BY CREATED_AT DESC
        """
        settings_df = session.sql(query_sql).collect()
        
        if settings_df:
            col1, col2 = st.columns([3, 1])
            
            with col1:
                settings_names = [f"{row['NAME']} ({row['CREATED_AT']})" for row in settings_df]
                selected_setting = st.selectbox("Select Settings to Load", settings_names)
                
                # Use horizontal radio button to choose action
                action = st.radio(
                    "Action:",
                    ["Review Settings", "Load Directly"],
                    horizontal=True,
                    key="settings_action"
                )
                
                if st.button("Execute Action", key="execute_action"):
                    setting_name = selected_setting.split(' (')[0]
                    
                    for row in settings_df:
                        if row['NAME'] == setting_name:
                            saved_data = json.loads(row['DATA_JSON'])
                            
                            if action == "Review Settings":
                                st.session_state['settings_to_load'] = saved_data
                                st.session_state['show_confirm_button'] = True
                            else:  # Load Directly
                                try:
                                    st.write("Loading settings...")
            
                                    # Handle config and timestamp first
                                    if 'config' in saved_data:
                                        config = saved_data['config']
                                        
                                        # Initialize a fresh config
                                        st.session_state['config'] = config
                                        
                                        # Handle timestamp initialization from saved settings
                                        if config.get('current_timestamp'):
                                            try:
                                                timestamp = datetime.fromisoformat(config['current_timestamp'])
                                                st.session_state['current_timestamp'] = timestamp
                                                st.session_state['selected_date'] = timestamp.date()
                                                st.session_state['selected_hour'] = timestamp.hour
                                                st.session_state['selected_minute'] = timestamp.minute
                                                config['current_timestamp'] = timestamp
                                            except (TypeError, ValueError) as e:
                                                st.error(f"Error converting timestamp: {e}")
                                                st.session_state['current_timestamp'] = datetime.now()
            
                                    # Load table info
                                    if 'table_info' in saved_data:
                                        st.session_state['selected_table_info'] = saved_data['table_info']
            
                                    # Load machine configs without touching measure selections
                                    if 'machine_configs' in saved_data:
                                        machine_configs = saved_data['machine_configs']
                                        st.session_state['machine_configs'] = {}
                                        
                                        for machine_name, machine_config in machine_configs.items():
                                            if 'measure_columns' in machine_config and 'settings' in machine_config:
                                                st.session_state['machine_configs'][machine_name] = {
                                                    'measure_columns': machine_config['measure_columns'],
                                                    'settings': machine_config['settings']
                                                }
                                                
                                                # Initialize measure selections in session state
                                                st.session_state[f"selected_measures_{machine_name}"] = machine_config['measure_columns']
                                                
                                                # Instead of directly setting widget values, store the settings to be used as defaults
                                                st.session_state[f"default_settings_{machine_name}"] = machine_config['settings']
            
                                    # Load special columns configuration
                                    if 'special_columns' in saved_data:
                                        special_cols = saved_data['special_columns']
                                        if 'config' not in st.session_state:
                                            st.session_state['config'] = {}
                                        st.session_state['config'].update({
                                            'machine_name_column': special_cols.get('machine_name_column'),
                                            'batch_id_column': special_cols.get('batch_id_column'),
                                            'timestamp_column': special_cols.get('timestamp_column'),
                                            'timestamp_mode': special_cols.get('timestamp_mode')
                                        })
            
                                    # Reset runtime state
                                    st.session_state['running'] = False
                                    st.session_state['current_measure_values'] = {}
                                    st.session_state['total_rows_generated'] = 0
                                    st.session_state['current_session_rows'] = 0
                                    st.session_state['show_confirm_button'] = False
                                    st.session_state['settings_to_load'] = None
            
                                    st.success("Settings loaded successfully!")
                                    time_module.sleep(1)
                                    st.rerun()
                                    
                                except Exception as e:
                                    st.error(f"Error loading settings: {str(e)}")
                                    st.error(f"Error type: {type(e)}")
                                    import traceback
                                    st.error("Full error trace:")
                                    st.code(traceback.format_exc())
                            break
                
                if st.session_state.get('show_confirm_button', False) and st.session_state.get('settings_to_load'):
                    saved_data = st.session_state['settings_to_load']
                    
                    st.write("### Settings to be loaded:")
                    
                    if 'table_info' in saved_data:
                        st.write("Table Information:")
                        st.json(saved_data['table_info'])
                    
                    if 'config' in saved_data:
                        st.write("Configuration:")
                        st.json(saved_data['config'])
                    
                    if 'machine_configs' in saved_data:
                        st.write("Machine Configurations:")
                        st.json(saved_data['machine_configs'])

                    if 'special_columns' in saved_data:
                        st.write("Special Columns Configuration:")
                        st.json(saved_data['special_columns'])
                    
                    if st.button("Confirm and Load These Settings", key="confirm_load"):
                        try:
                            st.write("Loading settings...")

                            # Handle config and timestamp first
                            if 'config' in saved_data:
                                config = saved_data['config']
                                
                                # Initialize a fresh config
                                st.session_state['config'] = config
                                
                                # Handle timestamp initialization from saved settings
                                if config.get('current_timestamp'):
                                    try:
                                        timestamp = datetime.fromisoformat(config['current_timestamp'])
                                        st.session_state['current_timestamp'] = timestamp
                                        st.session_state['selected_date'] = timestamp.date()
                                        st.session_state['selected_hour'] = timestamp.hour
                                        st.session_state['selected_minute'] = timestamp.minute
                                        config['current_timestamp'] = timestamp
                                    except (TypeError, ValueError) as e:
                                        st.error(f"Error converting timestamp: {e}")
                                        st.session_state['current_timestamp'] = datetime.now()

                            # Load table info
                            if 'table_info' in saved_data:
                                st.session_state['selected_table_info'] = saved_data['table_info']

                            # Load machine configs without touching measure selections
                            if 'machine_configs' in saved_data:
                                machine_configs = saved_data['machine_configs']
                                st.session_state['machine_configs'] = {}
                                
                                for machine_name, machine_config in machine_configs.items():
                                    if 'measure_columns' in machine_config and 'settings' in machine_config:
                                        st.session_state['machine_configs'][machine_name] = {
                                            'measure_columns': machine_config['measure_columns'],
                                            'settings': machine_config['settings']
                                        }
                                        
                                        # Initialize measure selections in session state
                                        st.session_state[f"selected_measures_{machine_name}"] = machine_config['measure_columns']
                                        
                                        # Instead of directly setting widget values, store the settings to be used as defaults
                                        st.session_state[f"default_settings_{machine_name}"] = machine_config['settings']

                            # Load special columns configuration
                            if 'special_columns' in saved_data:
                                special_cols = saved_data['special_columns']
                                if 'config' not in st.session_state:
                                    st.session_state['config'] = {}
                                st.session_state['config'].update({
                                    'machine_name_column': special_cols.get('machine_name_column'),
                                    'batch_id_column': special_cols.get('batch_id_column'),
                                    'timestamp_column': special_cols.get('timestamp_column'),
                                    'timestamp_mode': special_cols.get('timestamp_mode')
                                })

                            # Reset runtime state
                            st.session_state['running'] = False
                            st.session_state['current_measure_values'] = {}
                            st.session_state['total_rows_generated'] = 0
                            st.session_state['current_session_rows'] = 0
                            st.session_state['show_confirm_button'] = False
                            st.session_state['settings_to_load'] = None

                            st.success("Settings loaded successfully!")
                            time_module.sleep(1)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Error loading settings: {str(e)}")
                            st.error(f"Error type: {type(e)}")
                            import traceback
                            st.error("Full error trace:")
                            st.code(traceback.format_exc())
            
            with col2:
                if st.button("View Raw Data", key="view_data_button"):
                    setting_name = selected_setting.split(' (')[0]
                    for row in settings_df:
                        if row['NAME'] == setting_name:
                            st.subheader("Raw Settings Data")
                            saved_data = json.loads(row['DATA_JSON'])
                            st.json(saved_data)
                    
        else:
            st.warning(f"No settings found in {settings_db}.{settings_schema}.SETTINGS_TABLE")
            
    except Exception as e:
        st.error(f"Error in load_saved_settings: {str(e)}")
        st.error(f"Error type: {type(e)}")
        import traceback
        st.error("Full error trace:")
        st.code(traceback.format_exc())

def create_measure_inputs(columns, machine_name_column, batch_id_column, timestamp_column, machine_name, is_first_machine, first_machine_settings=None):
    # Get saved configuration if it exists
    saved_machine_config = st.session_state.get('machine_configs', {}).get(machine_name, {})
    
    # Filter out special columns
    excluded_columns = [col for col in [machine_name_column, batch_id_column, timestamp_column] if col is not None]
    measure_columns = [col for col in columns if col not in excluded_columns]
    
    # Create a persistent key for storing selected measures
    measure_state_key = f"selected_measures_{machine_name}"
    
    # Initialize measure selection state
    if measure_state_key not in st.session_state:
        if saved_machine_config and 'measure_columns' in saved_machine_config:
            st.session_state[measure_state_key] = list(saved_machine_config['measure_columns'])
        else:
            st.session_state[measure_state_key] = []

    # Create multiselect callback
    def on_measure_select():
        selected = st.session_state[f"multiselect_widget_{machine_name}"]
        st.session_state[measure_state_key] = list(selected)

    # Create multiselect for measure selection with callback
    selected_measures = st.multiselect(
        f"Select Measures for {machine_name}",
        options=measure_columns,
        default=st.session_state[measure_state_key],
        key=f"multiselect_widget_{machine_name}",
        on_change=on_measure_select
    )

    # Initialize machine settings dictionary
    machine_settings = {}

    # Configure each selected measure
    for measure in selected_measures:
        st.markdown(f"### {measure} Settings")
        
        data_type = st.session_state['selected_table_info']['column_types'].get(measure, 'VARCHAR')
        st.markdown(f"*Data Type: {data_type}*")
        
        integer_types = ['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT', 'NUMBER']
        is_integer = any(int_type in data_type.upper() and '(' not in data_type.upper() for int_type in integer_types)
        
        # Get saved settings for this measure
        saved_measure_settings = (saved_machine_config.get('settings', {}) or {}).get(measure, {})
        
        # Initialize all measure-specific state values
        state_keys = [
            f"include_{machine_name}_{measure}",
            f"mode_{machine_name}_{measure}",
            f"nom_min_{machine_name}_{measure}",
            f"nom_max_{machine_name}_{measure}",
            f"total_min_{machine_name}_{measure}",
            f"total_max_{machine_name}_{measure}",
            f"out_of_range_{machine_name}_{measure}",
            f"initial_{machine_name}_{measure}",
            f"max_{machine_name}_{measure}",
            f"increment_{machine_name}_{measure}"
        ]
        
        # Initialize session state for each key if not already present
        for key in state_keys:
            if key not in st.session_state:
                if 'include' in key:
                    st.session_state[key] = saved_measure_settings.get('include', True)
                elif 'mode' in key:
                    saved_mode = saved_measure_settings.get('mode', 'random')
                    st.session_state[key] = 0 if saved_mode == 'random' else 1
                elif 'nom_min' in key:
                    st.session_state[key] = saved_measure_settings.get('nominal_min', 0 if is_integer else 0.0)
                elif 'nom_max' in key:
                    st.session_state[key] = saved_measure_settings.get('nominal_max', 100 if is_integer else 100.0)
                elif 'total_min' in key:
                    default_min = st.session_state.get(f"nom_min_{machine_name}_{measure}", 0)
                    st.session_state[key] = saved_measure_settings.get('total_min', default_min - (5 if is_integer else 0.5))
                elif 'total_max' in key:
                    default_max = st.session_state.get(f"nom_max_{machine_name}_{measure}", 100)
                    st.session_state[key] = saved_measure_settings.get('total_max', default_max + (5 if is_integer else 0.5))
                elif 'out_of_range' in key:
                    st.session_state[key] = saved_measure_settings.get('percent_outside', 10)
                elif 'initial' in key:
                    st.session_state[key] = saved_measure_settings.get('initial_value', 0 if is_integer else 0.0)
                elif 'max' in key:
                    st.session_state[key] = saved_measure_settings.get('max_value', 100 if is_integer else 100.0)
                elif 'increment' in key:
                    st.session_state[key] = saved_measure_settings.get('increment', 1 if is_integer else 1.0)

        # Include checkbox - now using session state
        include_key = f"include_{machine_name}_{measure}"
        include_measure = st.checkbox(
            f"Include {measure}?",
            key=include_key
        )

        if include_measure:
            use_first_machine_settings = False
            if not is_first_machine and first_machine_settings and measure in first_machine_settings:
                use_first_machine_settings = st.checkbox(
                    f"Use same settings as first machine for {measure} (except % Outside Range)",
                    value=False,
                    key=f"use_first_{machine_name}_{measure}"
                )

            if use_first_machine_settings:
                first_settings = first_machine_settings[measure]
                mode = first_settings['mode']
                
                if mode == 'random':
                    st.text(f"Using settings from first machine:")
                    st.text(f"Nominal Range: {first_settings['nominal_min']} to {first_settings['nominal_max']}")
                    st.text(f"Total Range: {first_settings['total_min']} to {first_settings['total_max']}")
                    
                    percent_outside_key = f"out_of_range_{machine_name}_{measure}"
                    if percent_outside_key not in st.session_state:
                        st.session_state[percent_outside_key] = 10
                        
                    percent_outside = st.number_input(
                        f"% Outside Nominal Range",
                        min_value=0,
                        max_value=100,
                        key=percent_outside_key
                    )
                    
                    machine_settings[measure] = dict(first_settings)
                    machine_settings[measure]['percent_outside'] = percent_outside
                    machine_settings[measure]['data_type'] = data_type
                else:
                    st.text(f"Using settings from first machine:")
                    st.text(f"Initial Value: {first_settings['initial_value']}")
                    st.text(f"Maximum Value: {first_settings['max_value']}")
                    st.text(f"Increment: {first_settings['increment']}")
                    machine_settings[measure] = dict(first_settings)
                    machine_settings[measure]['data_type'] = data_type
            else:
                # Mode selection
                mode_key = f"mode_{machine_name}_{measure}"
                mode_options = ['random', 'additive']
                mode = mode_options[st.radio(
                    "Generation Mode",
                    options=range(len(mode_options)),
                    format_func=lambda x: mode_options[x],
                    key=mode_key
                )]

                if mode == 'random':
                    col1, col2 = st.columns(2)
                    with col1:
                        nom_min_key = f"nom_min_{machine_name}_{measure}"
                        nom_max_key = f"nom_max_{machine_name}_{measure}"
                        
                        if is_integer:
                            nominal_min = st.number_input(
                                "Nominal Min",
                                step=1,
                                key=nom_min_key
                            )
                            nominal_max = st.number_input(
                                "Nominal Max",
                                step=1,
                                key=nom_max_key
                            )
                        else:
                            nominal_min = st.number_input(
                                "Nominal Min",
                                step=0.1,
                                format="%.3f",
                                key=nom_min_key
                            )
                            nominal_max = st.number_input(
                                "Nominal Max",
                                step=0.1,
                                format="%.3f",
                                key=nom_max_key
                            )

                    with col2:
                        total_min_key = f"total_min_{machine_name}_{measure}"
                        total_max_key = f"total_max_{machine_name}_{measure}"
                        
                        if is_integer:
                            total_min = st.number_input(
                                "Total Min",
                                step=1,
                                key=total_min_key
                            )
                            total_max = st.number_input(
                                "Total Max",
                                step=1,
                                key=total_max_key
                            )
                        else:
                            total_min = st.number_input(
                                "Total Min",
                                step=0.1,
                                format="%.3f",
                                key=total_min_key
                            )
                            total_max = st.number_input(
                                "Total Max",
                                step=0.1,
                                format="%.3f",
                                key=total_max_key
                            )

                    percent_outside = st.number_input(
                        "% Outside Nominal Range",
                        min_value=0,
                        max_value=100,
                        step=1,
                        key=f"out_of_range_{machine_name}_{measure}"
                    )

                    machine_settings[measure] = {
                        'mode': 'random',
                        'nominal_min': nominal_min,
                        'nominal_max': nominal_max,
                        'total_min': total_min,
                        'total_max': total_max,
                        'percent_outside': percent_outside,
                        'include': True,
                        'data_type': data_type
                    }
                else:  # additive mode
                    col1, col2 = st.columns(2)
                    with col1:
                        initial_key = f"initial_{machine_name}_{measure}"
                        max_key = f"max_{machine_name}_{measure}"
                        
                        if is_integer:
                            initial_value = st.number_input(
                                "Initial Value",
                                step=1,
                                key=initial_key
                            )
                            max_value = st.number_input(
                                "Maximum Value",
                                step=1,
                                key=max_key
                            )
                        else:
                            initial_value = st.number_input(
                                "Initial Value",
                                step=0.1,
                                format="%.3f",
                                key=initial_key
                            )
                            max_value = st.number_input(
                                "Maximum Value",
                                step=0.1,
                                format="%.3f",
                                key=max_key
                            )

                    with col2:
                        increment_key = f"increment_{machine_name}_{measure}"
                        if is_integer:
                            increment = st.number_input(
                                "Increment per Row",
                                step=1,
                                key=increment_key
                            )
                        else:
                            increment = st.number_input(
                                "Increment per Row",
                                step=0.01,
                                format="%.3f",
                                key=increment_key
                            )

                    machine_settings[measure] = {
                        'mode': 'additive',
                        'initial_value': initial_value,
                        'max_value': max_value,
                        'increment': increment,
                        'include': True,
                        'data_type': data_type
                    }
        else:
            machine_settings[measure] = {'include': False}
        
        st.markdown("---")
    
    return selected_measures, machine_settings

def debug_state():
    """Helper function to debug session state"""
    if st.sidebar.checkbox("Show Debug Information", value=False):
        with st.sidebar.expander("Debug State", expanded=False):
            st.write({k: v for k, v in st.session_state.items() if not k.startswith('_')})
            
        if st.session_state.get('running', False):
            with st.sidebar.expander("Runtime Debug Info", expanded=False):
                st.write("Current Timestamp:", st.session_state.get('current_timestamp'))
                st.write("Machine Configs:", st.session_state.get('machine_configs', {}))
                st.write("Current Measure Values:", st.session_state.get('current_measure_values', {}))
    

def main():
    st.title("Snowflake Streaming Data Simulator")

    try:
        session = snowflake.snowpark.context.get_active_session()
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return

    # Create main navigation tabs
    tab_connection, tab_config, tab_machines, tab_generator = st.tabs([
        "📡 Connection", "⚙️ Configuration", "🔧 Machines", "▶️ Generator"
    ])

    with tab_connection:
        st.header("Database Connection")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            databases = get_databases(session)
            current_db = st.session_state['selected_table_info']['db']
            try:
                db_index = databases.index(current_db) if current_db in databases else 0
                selected_db = st.selectbox("Database", databases, index=db_index)
            except:
                selected_db = st.selectbox("Database", databases)

        if selected_db:
            with col2:
                schemas = get_schemas(session, selected_db)
                current_schema = st.session_state['selected_table_info']['schema']
                try:
                    schema_index = schemas.index(current_schema) if current_schema in schemas else 0
                    selected_schema = st.selectbox("Schema", schemas, index=schema_index)
                except:
                    selected_schema = st.selectbox("Schema", schemas)

            if selected_schema:
                with col3:
                    tables = get_tables(session, selected_db, selected_schema)
                    current_table = st.session_state['selected_table_info']['table']
                    try:
                        table_index = tables.index(current_table) if current_table in tables else 0
                        selected_table = st.selectbox("Table", tables, index=table_index)
                    except:
                        selected_table = st.selectbox("Table", tables)

                if selected_table:
                    if (selected_db != st.session_state['selected_table_info']['db'] or 
                        selected_schema != st.session_state['selected_table_info']['schema'] or 
                        selected_table != st.session_state['selected_table_info']['table']):
                        
                        columns_with_types = get_columns_with_types(session, selected_db, selected_schema, selected_table)
                        st.session_state['selected_table_info'] = {
                            'db': selected_db,
                            'schema': selected_schema,
                            'table': selected_table,
                            'columns': columns_with_types,
                            'column_types': {col: dtype for col, dtype in columns_with_types}
                        }
                        st.session_state['running'] = False
                        st.session_state['current_measure_values'] = {}
                        st.session_state['current_timestamp'] = None

                    columns_with_types = st.session_state['selected_table_info']['columns']
                    columns = [col[0] for col in columns_with_types]

    with tab_config:
        if selected_table:
            st.header("General Configuration")
            
            # Track selected special columns
            selected_special_columns = {
                'machine_name': None,
                'batch_id': None,
                'timestamp': None
            }

            # Helper function to get available columns
            def get_available_columns(current_field, selected_columns):
                excluded = [col for field, col in selected_columns.items() 
                          if col is not None and field != current_field]
                return ["None"] + [col for col in columns if col not in excluded]

            # Field Mapping Section
            with st.expander("📍 Field Mapping", expanded=True):
                col1, col2 = st.columns(2)
                with col1:
                    machine_name_options = get_available_columns('machine_name', selected_special_columns)
                    default_machine_name = st.session_state['config'].get('machine_name_column', "None")
                    if default_machine_name not in machine_name_options:
                        default_machine_name = "None"
                    machine_name_column = st.selectbox(
                        "Machine Name Column", 
                        machine_name_options,
                        index=machine_name_options.index(default_machine_name),
                        help="Column for machine names"
                    )
                    selected_special_columns['machine_name'] = None if machine_name_column == "None" else machine_name_column

                    batch_id_options = get_available_columns('batch_id', selected_special_columns)
                    default_batch_id = st.session_state['config'].get('batch_id_column', "None")
                    if default_batch_id not in batch_id_options:
                        default_batch_id = "None"
                    batch_id_column = st.selectbox(
                        "Batch ID Column", 
                        batch_id_options,
                        index=batch_id_options.index(default_batch_id),
                        help="Column for batch IDs"
                    )
                    selected_special_columns['batch_id'] = None if batch_id_column == "None" else batch_id_column

                with col2:
                    timestamp_columns = ["None"] + [col for col in columns 
                                                  if is_timestamp_type(st.session_state['selected_table_info']['column_types'].get(col, ''))]
                    default_timestamp = st.session_state['config'].get('timestamp_column', "None")
                    if default_timestamp not in timestamp_columns:
                        default_timestamp = "None"
                    timestamp_column = st.selectbox(
                        "Timestamp Column",
                        timestamp_columns,
                        index=timestamp_columns.index(default_timestamp),
                        help="Column for timestamps"
                    )
                    timestamp_column = None if timestamp_column == "None" else timestamp_column
                    selected_special_columns['timestamp'] = timestamp_column

                    if timestamp_column:
                        timestamp_data_type = st.session_state['selected_table_info']['column_types'][timestamp_column]
                        mode_key = f"timestamp_mode_{timestamp_column}"
                        default_timestamp_mode = st.session_state['config'].get('timestamp_mode', "Current")
                        
                        timestamp_mode = st.radio(
                            "Timestamp Mode",
                            ["Current", "Custom"],
                            index=0 if default_timestamp_mode == "Current" else 1,
                            key=mode_key,
                            help="Choose whether to use current system time or set a custom start time"
                        )

                        if timestamp_mode == 'Custom':
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                initial_date = st.date_input(
                                    "Initial Date", 
                                    value=st.session_state.get('selected_date', datetime.now().date()),
                                    key=f"date_input_{timestamp_column}"
                                )
                            with col2:
                                hour = st.number_input(
                                    "Hour (0-23)", 
                                    min_value=0, 
                                    max_value=23, 
                                    value=st.session_state.get('selected_hour', datetime.now().hour),
                                    key=f"hour_input_{timestamp_column}"
                                )
                            with col3:
                                minute = st.number_input(
                                    "Minute (0-59)", 
                                    min_value=0, 
                                    max_value=59, 
                                    value=st.session_state.get('selected_minute', datetime.now().minute),
                                    key=f"minute_input_{timestamp_column}"
                                )
                                
                            if not st.session_state.get('running'):
                                new_timestamp = datetime.combine(
                                    initial_date,
                                    time(hour=hour, minute=minute)
                                )
                                st.session_state['current_timestamp'] = new_timestamp
                                st.session_state['config']['current_timestamp'] = new_timestamp
                                st.session_state['config']['timestamp_mode'] = 'Custom'

            # Batch Settings Section
            with st.expander("📦 Batch Settings", expanded=True):
                col1, col2 = st.columns(2)
                with col1:
                    batch_id = st.text_input("Batch ID", value=st.session_state['config'].get('batch_id', "BATCH_001"))
                with col2:
                    write_frequency = st.number_input("Write Frequency (seconds)", 
                                                    min_value=1, 
                                                    value=st.session_state['config'].get('write_frequency', 5))

            # Save/Load Settings Section
            with st.expander("💾 Settings Management", expanded=True):
                col1, col2 = st.columns(2)
                with col1:
                    save_settings()
                with col2:
                    load_saved_settings()

    with tab_machines:
        if selected_table:
            st.header("Machine Configuration")
            
            # Machine Names Section
            with st.expander("🏭 Machine Names", expanded=True):
                num_machines = st.number_input("Number of Machines", 
                                             min_value=1, 
                                             max_value=10, 
                                             value=len(st.session_state['config'].get('machine_names', ['Machine_1'])))
                
                cols = st.columns(3)
                machine_names = []
                for i in range(num_machines):
                    col_index = i % 3
                    with cols[col_index]:
                        default_name = st.session_state['config'].get('machine_names', [])[i] if i < len(st.session_state['config'].get('machine_names', [])) else f"Machine_{i+1}"
                        machine_name = st.text_input(f"Machine {i+1}", value=default_name)
                        machine_names.append(machine_name)

            # Machine Measures Section
            st.subheader("Machine Measures")
            machine_configs = {}
            first_machine_settings = None
            available_measure_columns = [col for col in columns 
                                       if col not in [v for v in selected_special_columns.values() if v is not None]]
            
            for i, machine_name in enumerate(machine_names):
                with st.expander(f"📊 {machine_name} Measures", expanded=(i==0)):
                    measure_columns, machine_settings = create_measure_inputs(
                        available_measure_columns,
                        machine_name_column,
                        batch_id_column,
                        timestamp_column,
                        machine_name,
                        i == 0,
                        first_machine_settings if i > 0 else None
                    )
                    
                    if i == 0:
                        first_machine_settings = machine_settings
                        
                    machine_configs[machine_name] = {
                        'measure_columns': measure_columns,
                        'settings': machine_settings
                    }
                    
                    if 'machine_configs' not in st.session_state:
                        st.session_state['machine_configs'] = {}
                    st.session_state['machine_configs'][machine_name] = machine_configs[machine_name]

    with tab_generator:
        if selected_table:
            st.header("Generator Controls")
            
            # Status Display
            status_cols = st.columns(4)
            with status_cols[0]:
                st.metric("Total Rows", st.session_state['total_rows_generated'])
            with status_cols[1]:
                st.metric("Session Rows", st.session_state['current_session_rows'])
            with status_cols[2]:
                st.metric("Status", "Running" if st.session_state['running'] else "Stopped")
            
            # Control Buttons
            control_cols = st.columns(3)
            if not st.session_state['running']:
                if control_cols[0].button("▶️ Start", use_container_width=True):
                    st.session_state['machine_configs'] = machine_configs
                    st.session_state['running'] = True
                    st.session_state['last_batch_time'] = time_module.time()
                    st.session_state['current_session_rows'] = 0
                    st.session_state['current_measure_values'] = {}
            else:
                if control_cols[0].button("⏹️ Stop", use_container_width=True):
                    st.session_state['running'] = False
                    st.rerun()

            
            
            # Out-of-Range Settings
            # Runtime Settings
            if st.session_state['running']:
                with st.expander("⚠️ Runtime Settings", expanded=False):
                    tabs = st.tabs(["Random Mode", "Additive Mode"])
                    
                    with tabs[0]:  # Random Mode tab
                        st.header("% Outside Nominal Range")
                        has_random_measures = False
                        
                        for machine_name in st.session_state['machine_configs']:
                            random_measures = [m for m, s in st.session_state['machine_configs'][machine_name]['settings'].items() 
                                              if s.get('include', False) and s.get('mode') == 'random']
                            
                            if random_measures:
                                has_random_measures = True
                                st.subheader(machine_name)
                                cols = st.columns(3)
                                col_idx = 0
                                for measure in random_measures:
                                    measure_settings = st.session_state['machine_configs'][machine_name]['settings'][measure]
                                    with cols[col_idx % 3]:
                                        new_value = st.slider(
                                            f"{measure}",
                                            min_value=0,
                                            max_value=100,
                                            value=int(measure_settings['percent_outside']),
                                            key=f"slider_{machine_name}_{measure}"
                                        )
                                        if new_value != measure_settings['percent_outside']:
                                            st.session_state['machine_configs'][machine_name]['settings'][measure]['percent_outside'] = new_value
                                    col_idx += 1
                        
                        if not has_random_measures:
                            st.info("No random mode measures are currently active.")
                    
                    with tabs[1]:  # Additive Mode tab
                        st.header("Increment Settings")
                        has_additive_measures = False
                        
                        for machine_name in st.session_state['machine_configs']:
                            additive_measures = [m for m, s in st.session_state['machine_configs'][machine_name]['settings'].items() 
                                               if s.get('include', False) and s.get('mode') == 'additive']
                            
                            if additive_measures:
                                has_additive_measures = True
                                st.subheader(machine_name)
                                cols = st.columns(3)
                                col_idx = 0
                                for measure in additive_measures:
                                    measure_settings = st.session_state['machine_configs'][machine_name]['settings'][measure]
                                    data_type = measure_settings.get('data_type', 'FLOAT')
                                    is_integer = any(int_type in data_type.upper() and '(' not in data_type.upper() 
                                                    for int_type in ['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT', 'NUMBER'])
                                    
                                    with cols[col_idx % 3]:
                                        current_increment = measure_settings.get('increment', 1 if is_integer else 1.0)
                                        
                                        if is_integer:
                                            new_increment = st.number_input(
                                                f"{measure} Increment",
                                                min_value=0,
                                                step=1,
                                                value=int(current_increment),
                                                key=f"runtime_increment_{machine_name}_{measure}"  # Added 'runtime_' prefix
                                            )
                                        else:
                                            new_increment = st.number_input(
                                                f"{measure} Increment",
                                                min_value=0.0,
                                                step=0.1,
                                                format="%.3f",
                                                value=float(current_increment),
                                                key=f"runtime_increment_{machine_name}_{measure}"  # Added 'runtime_' prefix
                                            )
                                        
                                        if new_increment != current_increment:
                                            st.session_state['machine_configs'][machine_name]['settings'][measure]['increment'] = new_increment
                                            
                                        # Display current value
                                        measure_key = f"{machine_name}_{measure}"
                                        current_val = st.session_state['current_measure_values'].get(measure_key, "N/A")
                                        st.text(f"Current value: {current_val}")
                                    col_idx += 1
                        
                        if not has_additive_measures:
                            st.info("No additive mode measures are currently active.")

            # Update config state
            st.session_state['config'].update({
                'machine_name_column': machine_name_column if machine_name_column != "None" else None,
                'batch_id_column': batch_id_column if batch_id_column != "None" else None,
                'timestamp_column': timestamp_column,
                'timestamp_data_type': timestamp_data_type if timestamp_column else None,
                'timestamp_mode': timestamp_mode if timestamp_column else None,
                'write_frequency': write_frequency,
                'selected_db': selected_db,
                'selected_schema': selected_schema,
                'selected_table': selected_table,
                'machine_names': machine_names,
                'batch_id': batch_id,
                'current_timestamp': st.session_state.get('current_timestamp')
            })

            # Data generation logic
            if st.session_state['running']:
                current_time = time_module.time()
                
                if current_time - st.session_state['last_batch_time'] >= st.session_state['config']['write_frequency']:
                    # Handle timestamp increment
                    if st.session_state['config'].get('timestamp_column'):
                        current_mode = st.session_state['config'].get('timestamp_mode')
                        write_frequency = st.session_state['config']['write_frequency']
                        
                        if current_mode == 'Current':
                            st.session_state['current_timestamp'] = datetime.now()
                        elif current_mode == 'Custom':
                            try:
                                current_ts = st.session_state['current_timestamp']
                                if isinstance(current_ts, str):
                                    current_ts = datetime.fromisoformat(current_ts)
                                
                                # Increment timestamp
                                new_ts = current_ts + timedelta(seconds=write_frequency)
                                
                                # Update timestamp state
                                st.session_state['current_timestamp'] = new_ts
                                st.session_state['config']['current_timestamp'] = new_ts
                                
                            except Exception as e:
                                st.error(f"Error incrementing timestamp: {e}")
                                st.error("Current timestamp state:", st.session_state.get('current_timestamp'))
                                st.session_state['running'] = False
                            
                            # Update all timestamp-related state atomically
                            st.session_state['current_timestamp'] = new_ts
                            st.session_state['selected_date'] = new_ts.date()
                            st.session_state['selected_hour'] = new_ts.hour
                            st.session_state['selected_minute'] = new_ts.minute
                            st.session_state['config']['current_timestamp'] = new_ts
                            st.session_state['config']['timestamp_mode'] = 'Custom'  # Ensure mode stays Custom
                        
                    if st.session_state.get('current_timestamp') and st.sidebar.checkbox("Show Timestamp Debug", value=False, key="timestamp_debug_checkbox"):
                        st.sidebar.write("Current Timestamp:", st.session_state['current_timestamp'])

                    # Generate batch rows
                    batch_rows = []
                    for machine_name in st.session_state['config']['machine_names']:
                        machine_config = st.session_state['machine_configs'][machine_name]
                        
                        # Initialize row data
                        row_data = {col: None for col in columns}
                        
                        if st.session_state['config']['machine_name_column']:
                            row_data[st.session_state['config']['machine_name_column']] = machine_name
                        
                        if st.session_state['config']['batch_id_column']:
                            row_data[st.session_state['config']['batch_id_column']] = st.session_state['config']['batch_id']
                        
                        if st.session_state['config']['timestamp_column']:
                            row_data[st.session_state['config']['timestamp_column']] = format_timestamp_for_snowflake(
                                st.session_state['current_timestamp'],
                                st.session_state['config']['timestamp_data_type']
                            )
                        
                        for measure in machine_config['measure_columns']:
                            settings = machine_config['settings'].get(measure, {})
                            if settings.get('include', False):
                                measure_key = f"{machine_name}_{measure}"
                                
                                if settings['mode'] == 'additive':
                                    if measure_key not in st.session_state['current_measure_values']:
                                        st.session_state['current_measure_values'][measure_key] = settings['initial_value']
                                    
                                    current_value = st.session_state['current_measure_values'][measure_key]
                                    new_value = generate_measure_value(settings, settings['data_type'], current_value)
                                    row_data[measure] = new_value
                                    st.session_state['current_measure_values'][measure_key] = new_value
                                else:
                                    row_data[measure] = generate_measure_value(settings, settings['data_type'])
                        
                        batch_rows.append(row_data)
                        st.session_state['total_rows_generated'] += 1
                        st.session_state['current_session_rows'] += 1

                    if batch_rows:
                        df = pd.DataFrame(batch_rows)
                        try:
                            session = snowflake.snowpark.context.get_active_session()
                            success = write_to_snowflake(
                                session, 
                                df, 
                                st.session_state['config']['selected_db'], 
                                st.session_state['config']['selected_schema'], 
                                st.session_state['config']['selected_table']
                            )
                            if not success:
                                st.session_state['running'] = False
                        except Exception as e:
                            st.error(f"Failed to connect to Snowflake: {str(e)}")
                            st.session_state['running'] = False
                    
                    st.session_state['last_batch_time'] = time_module.time()

                time_module.sleep(0.1)
                
                if st.session_state['running']:
                    st.rerun()

if __name__ == "__main__":
    main()
    debug_state()