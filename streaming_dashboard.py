import streamlit as st
import time
import pandas as pd
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import json
import re
import _snowflake
import html

# Set page config with enhanced options
st.set_page_config(
    page_title="Mill Machine Dashboard",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom CSS for improved UI
st.markdown("""
<style>
    /* Main styling */
    .main {
        background-color: #f8f9fa;
    }
    
    /* Card styling */
    .card {
        background-color: white;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
        margin-bottom: 20px;
    }
    
    /* Section headings */
    .section-heading {
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: 16px;
        color: #1f2937;
        padding-bottom: 8px;
        border-bottom: 1px solid #e5e7eb;
    }
    
    /* Dashboard header */
    .dashboard-header {
        background-color: #1f2937;
        color: white;
        padding: 12px 20px;
        border-radius: 8px;
        margin-bottom: 20px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    
    /* Status indicators */
    .status-indicator {
        display: inline-block;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        margin-right: 6px;
    }
    .status-green {
        background-color: #10b981;
    }
    .status-yellow {
        background-color: #f59e0b;
    }
    .status-red {
        background-color: #ef4444;
    }
    
    /* Metrics panel */
    .metrics-panel {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-bottom: 20px;
    }
    .metric-card {
        flex: 1;
        min-width: 150px;
        background-color: white;
        padding: 16px;
        border-radius: 8px;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.05);
        text-align: center;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 600;
        margin: 8px 0;
    }
    .metric-title {
        font-size: 0.9rem;
        color: #6b7280;
        margin-bottom: 4px;
    }
    
    /* Improved tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        border-radius: 4px 4px 0 0;
        padding: 0 20px;
        background-color: #f3f4f6;
    }
    .stTabs [aria-selected="true"] {
        background-color: white;
        border-bottom: 3px solid #1f77b4;
    }
    
    /* Chat container for Mill Machine Assistant */
    .chat-container {
        border-radius: 8px;
        background-color: white;
        height: 600px;
        overflow-y: auto;
        padding: 16px;
        box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
    }
    
    /* Better tooltips */
    .tooltip {
        position: relative;
        display: inline-block;
        cursor: help;
    }
    .tooltip .tooltiptext {
        visibility: hidden;
        width: 200px;
        background-color: #1f2937;
        color: white;
        text-align: center;
        border-radius: 6px;
        padding: 8px;
        position: absolute;
        z-index: 1;
        bottom: 125%;
        left: 50%;
        margin-left: -100px;
        opacity: 0;
        transition: opacity 0.3s;
    }
    .tooltip:hover .tooltiptext {
        visibility: visible;
        opacity: 1;
    }
    
    /* Last updated indicator */
    .last-updated {
        font-size: 0.8rem;
        color: #6b7280;
        text-align: right;
        margin-top: 4px;
    }
    
    /* Improve Streamlit components */
    .stButton button {
        border-radius: 4px;
        font-weight: 600;
        transition: all 0.2s;
    }
    .stButton button:hover {
        transform: translateY(-1px);
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
    }
    .stTextInput > div > div > input {
        border-radius: 4px;
    }
    .stSelectbox > div > div {
        border-radius: 4px;
    }
    
    /* Loading animation */
    @keyframes pulse {
        0% { opacity: 0.6; }
        50% { opacity: 0.8; }
        100% { opacity: 0.6; }
    }
    .loading {
        animation: pulse 1.5s infinite;
        background-color: #e5e7eb;
        border-radius: 4px;
    }
    
    /* Fix shadow persistence - more aggressive approach */
    [data-testid="stVerticalBlock"] {
        box-shadow: none !important;
    }
    .css-1544g2n {
        box-shadow: none !important;
    }
    .css-18e3th9 {
        box-shadow: none !important;
    }
    .css-1d391kg {
        box-shadow: none !important;
    }
    div.css-1r6slb0.e1tzin5v2 {
        box-shadow: none !important;
    }
    div.css-keje6w.e1tzin5v2 {
        box-shadow: none !important;
    }
    div[data-testid="stDecoration"] {
        box-shadow: none !important;
        background: none !important;
    }
    /* Remove all shadows from all elements */
    * {
        box-shadow: none !important;
    }
    /* Exception for our specific cards */
    .card {
        box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08) !important;
    }
    .metric-card {
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.05) !important;
    }
</style>
""", unsafe_allow_html=True)

# Define nominal ranges
NOMINAL_RANGES = {
    'SPINDLE_SPEED': (2800, 3200),
    'VIBRATION': (0.1, 0.3),
    'FEED_RATE': (100, 120),
    'TOOL_WEAR': (0, 50)
}

# Define a list of distinct colors to cycle through
DISTINCT_COLORS = [
    "#1f77b4",  # blue
    "#ff7f0e",  # orange
    "#2ca02c",  # green
    "#d62728",  # red
    "#9467bd",  # purple
    "#8c564b",  # brown
    "#e377c2",  # pink
    "#7f7f7f",  # gray
    "#bcbd22",  # olive
    "#17becf",  # teal
]

# API settings for Cortex Analyst
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000  # in milliseconds
CORTEX_SEARCH_SERVICES = "demo_db.streaming.mill_ss2"  # Updated to use the better mill_ss2 service
SEMANTIC_MODELS = "@demo_db.streaming.my_stage/machine_performance.yaml"

# Initialize Snowflake session
try:
    session = get_active_session()
except Exception as e:
    st.error(f"Error connecting to Snowflake: {str(e)}")
    st.stop()

# Display enhanced header
st.markdown("""
<div class="dashboard-header">
    <div style="background-color:#1f2937; color:white; padding:12px 20px; border-radius:8px; margin-bottom:20px; display:flex; align-items:center; justify-content:space-between;">
        <div>
            <h1 style="margin:0; display:flex; align-items:center; color:white;">
                <span style="margin-right:10px;">🏭</span> 
                Real-Time Mill Monitoring Dashboard
            </h1>
        </div>
        <div>
            <span id="current-time" style="font-size:1.1rem; opacity:0.9; color:white;">
                {current_time}
            </span>
        </div>
    </div>
""".format(current_time=datetime.now().strftime("%a %b %d, %Y - %H:%M:%S")), unsafe_allow_html=True)

# Load machine names function
@st.cache_data(ttl=60)
def load_machine_names():
    try:
        query = "SELECT DISTINCT MACHINE_NAME FROM DEMO_DB.STREAMING.MACHINE_TBL"
        machines = session.sql(query).to_pandas()['MACHINE_NAME'].tolist()
        if machines:
            return machines
        else:
            return ["Machine1", "Machine2", "Machine3", "Machine4"]
    except Exception as e:
        st.sidebar.error(f"Error loading machine names: {str(e)}")
        return ["Machine1", "Machine2", "Machine3", "Machine4"]

# Function to load the latest sensor data
def load_sensor_data():
    try:
        if not st.session_state.selected_machines:
            return pd.DataFrame()
            
        # Calculate the time window cutoff
        time_window_minutes = st.session_state.time_window_minutes
            
        # Construct query with specific machine filter and time window
        machine_list = ", ".join(f"'{m}'" for m in st.session_state.selected_machines)
        query = f"""
        SELECT TIMESTAMP, MACHINE_NAME, SPINDLE_SPEED, VIBRATION, FEED_RATE, TOOL_WEAR
        FROM DEMO_DB.STREAMING.MACHINE_TBL
        WHERE MACHINE_NAME IN ({machine_list})
        AND TIMESTAMP >= DATEADD(minute, -{time_window_minutes}, CURRENT_TIMESTAMP())
        ORDER BY TIMESTAMP ASC
        """
        
        # Execute query
        df = session.sql(query).to_pandas()
        
        # Convert timestamp to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['TIMESTAMP']):
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
            
        # Ensure numeric columns are numeric
        numeric_cols = ['SPINDLE_SPEED', 'VIBRATION', 'FEED_RATE', 'TOOL_WEAR']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

# Function to load predicted yield data
def load_predicted_yield():
    try:
        query = "select machine_name, predicted_yield from predictive_stats"
        df = session.sql(query).to_pandas()
        
        # Convert predicted_yield to numeric if needed
        if 'PREDICTED_YIELD' in df.columns:
            df['PREDICTED_YIELD'] = pd.to_numeric(df['PREDICTED_YIELD'], errors='coerce')
        elif 'predicted_yield' in df.columns:  # Check for lowercase column name
            df['predicted_yield'] = pd.to_numeric(df['predicted_yield'], errors='coerce')
            # Rename to uppercase for consistency
            df = df.rename(columns={'predicted_yield': 'PREDICTED_YIELD', 'machine_name': 'MACHINE_NAME'})
        
        # Ensure column names are uppercase
        df.columns = [col.upper() for col in df.columns]
        
        return df
    except Exception as e:
        st.error(f"Error loading predicted yield data: {str(e)}")
        return pd.DataFrame()

# Function to load machine stats data with styling
def load_machine_stats():
    try:
        query = "select * from machine_stats"
        df = session.sql(query).to_pandas()
        
        # Convert numeric columns to proper types first
        for col in df.columns:
            if col.upper() != 'MACHINE_NAME' and df[col].dtype.name in ['object', 'string']:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except:
                    pass
        
        # Create a styled version with colors
        def highlight_cells(val, column):
            col_upper = column.upper()
            
            # Default style (no highlight)
            style = ''
            
            # For percentage columns
            if col_upper.endswith('_PCT'):
                if val > 50:
                    style = 'background-color: darkred; color: white'
                elif val > 30:
                    style = 'background-color: red; color: white'
                elif val > 15:
                    style = 'background-color: yellow'
                    
            # SPINDLE_SPEED_AVG
            elif col_upper == 'SPINDLE_SPEED_AVG':
                if val > 3500:
                    style = 'background-color: darkred; color: white'
                elif val > 3200:
                    style = 'background-color: red; color: white'
                elif val < 2500:
                    style = 'background-color: darkblue; color: white'
                elif val < 2800:
                    style = 'background-color: blue; color: white'
                    
            # VIBRATION_AVG
            elif col_upper == 'VIBRATION_AVG':
                if val > 0.8:
                    style = 'background-color: darkred; color: white'
                elif val > 0.3:
                    style = 'background-color: red; color: white'
                elif val < 0.05:
                    style = 'background-color: darkblue; color: white'
                elif val < 0.1:
                    style = 'background-color: blue; color: white'
                    
            # FEED_RATE_AVG
            elif col_upper == 'FEED_RATE_AVG':
                if val > 140:
                    style = 'background-color: darkred; color: white'
                elif val > 120:
                    style = 'background-color: red; color: white'
                elif val < 80:
                    style = 'background-color: darkblue; color: white'
                elif val < 100:
                    style = 'background-color: blue; color: white'
                    
            # TOOL_WEAR
            elif col_upper == 'TOOL_WEAR' or col_upper == 'TOOL_WEAR_AVG':
                if val > 80:
                    style = 'background-color: darkred; color: white'
                elif val > 70:
                    style = 'background-color: red; color: white'
                elif val > 50:
                    style = 'background-color: yellow'
                    
            return style
            
        # Add a status_summary to the session state
        if 'status_summary' not in st.session_state:
            st.session_state.status_summary = {
                'critical': 0,
                'warning': 0,
                'normal': 0
            }
            
        # Count machines in each status category
        critical_count = 0
        warning_count = 0
        normal_count = 0
        
        for _, row in df.iterrows():
            # Check if any values are critical or warnings
            has_critical = False
            has_warning = False
            
            for col in df.columns:
                if col.upper() == 'MACHINE_NAME':
                    continue
                
                val = row[col]
                if not isinstance(val, (int, float)) or pd.isna(val):
                    continue
                    
                # Check for critical values
                if (col.upper().endswith('_PCT') and val > 50) or \
                   (col.upper() == 'SPINDLE_SPEED_AVG' and (val > 3500 or val < 2500)) or \
                   (col.upper() == 'VIBRATION_AVG' and (val > 0.8 or val < 0.05)) or \
                   (col.upper() == 'FEED_RATE_AVG' and (val > 140 or val < 80)) or \
                   ((col.upper() == 'TOOL_WEAR' or col.upper() == 'TOOL_WEAR_AVG') and val > 80):
                    has_critical = True
                    break
                
                # Check for warning values
                if (col.upper().endswith('_PCT') and val > 30) or \
                   (col.upper() == 'SPINDLE_SPEED_AVG' and (val > 3200 or val < 2800)) or \
                   (col.upper() == 'VIBRATION_AVG' and (val > 0.3 or val < 0.1)) or \
                   (col.upper() == 'FEED_RATE_AVG' and (val > 120 or val < 100)) or \
                   ((col.upper() == 'TOOL_WEAR' or col.upper() == 'TOOL_WEAR_AVG') and val > 70):
                    has_warning = True
            
            # Increment counters
            if has_critical:
                critical_count += 1
            elif has_warning:
                warning_count += 1
            else:
                normal_count += 1
        
        # Update status summary
        st.session_state.status_summary = {
            'critical': critical_count,
            'warning': warning_count,
            'normal': normal_count
        }
        
        # Create a new HTML-formatted DataFrame
        html_parts = ['<table style="width:100%; border-collapse: collapse; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden;">']
        
        # Add header row
        html_parts.append('<tr>')
        for col in df.columns:
            html_parts.append(f'<th style="text-align:left;padding:10px;background-color:#f0f0f0;border:1px solid #ddd;">{col}</th>')
        html_parts.append('</tr>')
        
        # Add data rows
        for _, row in df.iterrows():
            html_parts.append('<tr>')
            
            for col in df.columns:
                val = row[col]
                
                # Special handling for machine name column
                if col.upper() == 'MACHINE_NAME':
                    # Ensure machine name has black text on white background
                    display_val = str(val) if not pd.isna(val) else ""
                    html_parts.append(f'<td style="padding:10px;border:1px solid #ddd;background-color:white;color:black;font-weight:bold;">{display_val}</td>')
                    continue
                
                # Default style
                style = ""
                
                # Apply highlight style if it's a numeric value
                if isinstance(val, (int, float)) and not pd.isna(val):
                    style = highlight_cells(val, col)
                
                # Format the value
                if isinstance(val, (int, float)):
                    if pd.isna(val):
                        display_val = ""
                    elif col.upper().endswith('_PCT'):
                        display_val = f"{val:.2f}%"
                    else:
                        display_val = f"{val:.2f}"
                else:
                    display_val = str(val) if not pd.isna(val) else ""
                
                html_parts.append(f'<td style="padding:10px;border:1px solid #ddd;{style}">{display_val}</td>')
            
            html_parts.append('</tr>')
        
        html_parts.append('</table>')
        html_table = ''.join(html_parts)
        
        return df, html_table
    except Exception as e:
        st.error(f"Error loading machine stats data: {str(e)}")
        return pd.DataFrame(), ""

# Create gauge chart for predicted yield
def create_gauge_chart(machine_name, yield_value):
    # Set default if value is None or NaN
    if pd.isna(yield_value):
        yield_value = 0
        
    # Check if the value is already a percentage or a decimal
    # If it's small (like 0.9325), multiply by 100 to convert from decimal to percentage
    # But if it's 1 or higher, assume it's already a percentage or an integer value
    if 0 < yield_value < 5:
        yield_value = yield_value * 100
    
    # Ensure value is between 0 and 100
    yield_value = max(0, min(100, yield_value))
    
    # Determine color based on thresholds - matches the alert thresholds
    if yield_value >= 95:
        color = "green"
        number_color = "black"  # Dark text for green background
    elif yield_value >= 92:
        color = "yellow"
        number_color = "black"  # Dark text on yellow for readability
    else:
        color = "#CC0000"  # red
        number_color = "black"  # White text on red for visibility
    
    # Create the gauge with enhanced styling
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=yield_value,
        number={"suffix": "%", "valueformat": ".2f", "font": {"size": 24, "color": number_color}},
        title={"text": machine_name, "font": {"size": 18, "family": "Arial, sans-serif"}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "darkgray", "tickfont": {"size": 12}},
            "bar": {"color": color},
            "bgcolor": "white",
            "borderwidth": 2,
            "bordercolor": "gray",
            "steps": [
                {"range": [0, 92], "color": "rgba(204, 0, 0, 0.1)"},      # red
                {"range": [92, 95], "color": "rgba(255, 255, 0, 0.1)"},   # yellow
                {"range": [95, 100], "color": "rgba(0, 128, 0, 0.1)"}     # green
            ],
            "threshold": {
                "line": {"color": "black", "width": 4},
                "thickness": 0.75,
                "value": yield_value
            }
        }
    ))
    
    # Add hover information
    hover_text = f"Machine: {machine_name}<br>Yield: {yield_value:.2f}%<br>Status: "
    if yield_value >= 95:
        hover_text += "Excellent"
    elif yield_value >= 92:
        hover_text += "Warning"
    else:
        hover_text += "Critical"
    
    # Enhance layout for better readability
    fig.update_layout(
        height=250,
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor="white",
        plot_bgcolor="white",
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Arial, sans-serif"
        ),
        annotations=[
            dict(
                text=hover_text,
                x=0.5,
                y=0.25,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=1, color="rgba(0,0,0,0)"),
                hovertext=hover_text
            )
        ]
    )
    
    return fig

# Function to create a chart for a specific measure
def create_chart(data, measure):
    # Create base figure
    fig = go.Figure()
    
    # Handle empty data case
    if data.empty:
        fig.update_layout(
            xaxis_title='Timestamp',
            yaxis_title=measure,
            height=300,
            annotations=[{
                'text': 'No Data Available',
                'showarrow': False,
                'font': {'size': 16}
            }]
        )
        return fig
    
    # Get unique machines actually present in the data
    machines_in_data = sorted(data['MACHINE_NAME'].unique())
    
    # Assign colors to any new machines
    if 'machine_color_map' not in st.session_state:
        st.session_state.machine_color_map = {}
        
    for machine in machines_in_data:
        if machine not in st.session_state.machine_color_map:
            # Assign the next available color from our palette
            color_index = len(st.session_state.machine_color_map) % len(DISTINCT_COLORS)
            st.session_state.machine_color_map[machine] = DISTINCT_COLORS[color_index]
    
    # Add a trace for each machine in a consistent order
    # Use all_machines list to maintain the original order
    for machine in all_machines:
        if machine in st.session_state.selected_machines and machine in machines_in_data:
            machine_data = data[data['MACHINE_NAME'] == machine]
            if not machine_data.empty:
                # Get color from our session state map
                color = st.session_state.machine_color_map[machine]
                
                # Add an enhanced trace with better hover info
                fig.add_trace(go.Scatter(
                    x=machine_data['TIMESTAMP'], 
                    y=machine_data[measure],
                    mode='lines+markers',
                    name=f'{machine}',
                    line=dict(width=2, color=color, shape='spline', smoothing=0.3),
                    marker=dict(size=6, color=color, line=dict(width=1, color='white')),
                    hovertemplate=f"<b>{machine}</b><br>" +
                                 f"Time: %{{x|%H:%M:%S}}<br>" +
                                 f"{measure}: %{{y:.2f}}<extra></extra>"
                ))
    
    # Add nominal range lines
    timestamps = data['TIMESTAMP'].unique()
    if len(timestamps) > 0:
        min_val, max_val = NOMINAL_RANGES[measure]
        
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=[min_val] * len(timestamps),
            mode='lines',
            name=f'Lower Limit ({min_val})',
            line=dict(color='red', width=1.5, dash='dash'),
            hoverinfo='name+y'
        ))
        
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=[max_val] * len(timestamps),
            mode='lines',
            name=f'Upper Limit ({max_val})',
            line=dict(color='red', width=1.5, dash='dash'),
            fill='tonexty',
            fillcolor='rgba(0, 255, 0, 0.1)',
            hoverinfo='name+y'
        ))
    
    # Layout - enhanced for better readability
    fig.update_layout(
        xaxis_title='Time',
        yaxis_title=measure,
        height=300,
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(
            orientation="h", 
            yanchor="bottom", 
            y=1.02, 
            xanchor="right", 
            x=1,
            traceorder="normal",
            font=dict(size=12),
            bordercolor="LightGrey",
            borderwidth=1
        ),
        hovermode="closest",
        plot_bgcolor='rgba(240, 240, 240, 0.5)',
        paper_bgcolor='white',
        font=dict(family="Arial, sans-serif"),
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(200, 200, 200, 0.2)',
            tickformat='%H:%M:%S',
            hoverformat='%H:%M:%S'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(200, 200, 200, 0.2)',
            tickformat='.2f'
        )
    )
    
    # Grid lines
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(200, 200, 200, 0.2)', zeroline=False)
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(200, 200, 200, 0.2)', zeroline=False)
    
    return fig

# Function to call Snowflake API
def snowflake_api_call(query: str, limit: int = 10):
    
    payload = {
        "model": "claude-3-5-sonnet",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": query
                    }
                ]
            }
        ],
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "analyst1"
                }
            },
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": "search1"
                }
            }
        ],
        "tool_resources": {
            "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
            "search1": {
                "name": CORTEX_SEARCH_SERVICES,
                "max_results": limit
            }
        }
    }
    
    try:
        resp = _snowflake.send_snow_api_request(
            "POST",  # method
            API_ENDPOINT,  # path
            {},  # headers
            {},  # params
            payload,  # body
            None,  # request_guid
            API_TIMEOUT,  # timeout in milliseconds,
        )
        
        if resp["status"] != 200:
            st.error(f"❌ HTTP Error: {resp['status']} - {resp.get('reason', 'Unknown reason')}")
            st.error(f"Response details: {resp}")
            return None
        
        try:
            response_content = json.loads(resp["content"])
        except json.JSONDecodeError:
            st.error("❌ Failed to parse API response. The server may have returned an invalid JSON format.")
            st.error(f"Raw response: {resp['content'][:200]}...")
            return None
            
        return response_content
            
    except Exception as e:
        st.error(f"Error making request: {str(e)}")
        return None

def process_sse_response(response):
    """Process SSE response"""
    text = ""
    sql = ""
    citations = []
    
    if not response:
        return text, sql, citations
    if isinstance(response, str):
        return text, sql, citations
        
    try:
        for event in response:
            if event.get('event') == "message.delta":
                data = event.get('data', {})
                delta = data.get('delta', {})
                
                for content_item in delta.get('content', []):
                    content_type = content_item.get('type')
                    if content_type == "tool_results":
                        tool_results = content_item.get('tool_results', {})
                        if 'content' in tool_results:
                            for result in tool_results['content']:
                                if result.get('type') == 'json':
                                    text += result.get('json', {}).get('text', '')
                                    search_results = result.get('json', {}).get('searchResults', [])
                                    for search_result in search_results:
                                        citations.append({'source_id':search_result.get('source_id',''), 'doc_id':search_result.get('doc_id', '')})
                                    sql = result.get('json', {}).get('sql', '')
                    if content_type == 'text':
                        text += content_item.get('text', '')
                            
    except json.JSONDecodeError as e:
        st.error(f"Error processing events: {str(e)}")
                
    except Exception as e:
        st.error(f"Error processing events: {str(e)}")
        
    return text, sql, citations

# Function to run SQL query
def run_snowflake_query(query):
    try:
        df = session.sql(query.replace(';',''))
        return df
    except Exception as e:
        st.error(f"Error executing SQL: {str(e)}")
        return None, None

# Load machine names
all_machines = load_machine_names()

# Create sidebar with enhanced styling
with st.sidebar:
    st.sidebar.markdown("""
    <div style="text-align:center; margin-bottom:20px;">
        <h3 style="margin:0; padding:10px; background-color:#1f2937; color:white; border-radius:8px;">
            Dashboard Controls
        </h3>
    </div>
    """, unsafe_allow_html=True)
    
    # Add a search box for machines
    search_term = st.text_input("🔍 Search Machines", placeholder="Type to filter machines...")
    
    # Filter machines based on search term
    filtered_machines = [m for m in all_machines if search_term.lower() in m.lower()] if search_term else all_machines
    
    st.markdown("<h4>Select Machines</h4>", unsafe_allow_html=True)

    # Initialize machine selection on first run
    if 'machine_selection_initialized' not in st.session_state:
        st.session_state.machine_selection_initialized = True
        st.session_state.selected_machines = all_machines.copy()

    # Buttons for selecting all or none with improved styling
    col1, col2 = st.columns(2)

    # Select All button
    if col1.button("Select All", use_container_width=True):
        st.session_state.selected_machines = filtered_machines.copy()
        st.rerun()

    # Clear All except first machine - renamed to "Select One" for clarity
    if col2.button("Select One", use_container_width=True):
        if filtered_machines:
            st.session_state.selected_machines = [filtered_machines[0]]
            st.rerun()

    # Display list with current selection status in a scrollable container
    st.markdown("""
    <style>
        .machine-list {
            max-height: 300px;
            overflow-y: auto;
            border: 1px solid #e5e7eb;
            border-radius: 4px;
            padding: 8px;
            margin-bottom: 10px;
        }
    </style>
    <div class="machine-list">
    """, unsafe_allow_html=True)
    
    for machine in filtered_machines:
        is_selected = machine in st.session_state.selected_machines
        
        # Create checkbox-like button with Unicode symbols and improved styling
        btn_label = f"✓ {machine}" if is_selected else f"□ {machine}"
        btn_style = "primary" if is_selected else "secondary"
        if st.button(btn_label, key=f"machine_btn_{machine}", use_container_width=True, type=btn_style):
            # Toggle selection state
            if is_selected and len(st.session_state.selected_machines) > 1:
                # Only remove if we have more than one selected
                st.session_state.selected_machines.remove(machine)
            elif not is_selected:
                # Add to selection
                st.session_state.selected_machines.append(machine)
            # Force refresh
            st.rerun()
    
    st.markdown("</div>", unsafe_allow_html=True)

    # Display selection status with enhanced styling
    selected_count = len(st.session_state.selected_machines)
    if selected_count == len(all_machines):
        st.success(f"✅ All {selected_count} machines selected")
    else:
        st.info(f"📊 {selected_count} of {len(all_machines)} machines selected")

    # Time range selector with improved UI
    st.markdown("---")
    st.markdown("<h4>Time Range</h4>", unsafe_allow_html=True)

    # Define time range presets
    time_presets = {
        "Last 5 minutes": 5,
        "Last 15 minutes": 15,
        "Last 30 minutes": 30,
        "Last hour": 60,
        "Last 3 hours": 180,
        "Last 6 hours": 360,
        "Last 12 hours": 720,
        "Last 24 hours": 1440
    }

    # Initialize time window and preset if needed
    if 'time_window_minutes' not in st.session_state:
        st.session_state.time_window_minutes = 60  # Default to 1 hour

    if 'selected_time_preset' not in st.session_state:
        st.session_state.selected_time_preset = "Last hour"  # Default preset

    # Find the current index based on time_window_minutes
    current_preset = st.session_state.selected_time_preset
    default_index = list(time_presets.keys()).index(current_preset) if current_preset in time_presets else 3

    # Radio button for preset selection with better styling
    selected_preset = st.radio(
        "Select time range:",
        options=list(time_presets.keys()),
        index=default_index,
        horizontal=False
    )

    # Update the time window and selected preset in session state
    st.session_state.time_window_minutes = time_presets[selected_preset]
    st.session_state.selected_time_preset = selected_preset

    # Show the actual minutes value with a tooltip
    st.caption(f"Showing data from the last {st.session_state.time_window_minutes} minutes")

    # Refresh interval control with enhanced UI
    st.markdown("---")
    st.markdown("<h4>Refresh Settings</h4>", unsafe_allow_html=True)

    # Initialize refresh interval if needed
    if 'refresh_interval' not in st.session_state:
        st.session_state.refresh_interval = 5  # Default to 5 seconds

    # Define refresh interval presets
    refresh_presets = {
        "Every 5 seconds": 5,
        "Every 10 seconds": 10,
        "Every 30 seconds": 30,
        "Every minute": 60,
        "Every 2 minutes": 120,
        "Every 5 minutes": 300
    }

    # Radio button for refresh interval
    selected_refresh = st.radio(
        "Refresh data:",
        options=list(refresh_presets.keys()),
        index=0,  # Default to "Every 5 seconds"
        horizontal=False
    )

    # Update the refresh interval based on selection
    st.session_state.refresh_interval = refresh_presets[selected_refresh]

    # Add a forced refresh button with better styling
    if st.button("Force Refresh Now", use_container_width=True, type="primary"):
        st.rerun()

    # Show last refresh time with a better format
    refresh_time = datetime.now().strftime("%H:%M:%S")
    st.markdown(f"""
    <div style="text-align:center; margin-top:10px; padding:5px; background-color:#f0f0f0; border-radius:4px;">
        <span style="font-size:12px; color:#666;">Last refreshed: {refresh_time}</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Dashboard auto-refresh toggle
    st.markdown("---")
    enable_auto_refresh = st.checkbox("Enable auto-refresh", value=True, 
                                    help="When checked, the Sensor Dashboard will auto-refresh based on the interval above.")

# Initialize tab state
if "active_tab" not in st.session_state:
    st.session_state.active_tab = "Sensor Dashboard"  # Default to first tab

# Initialize chat history if it doesn't exist
if "messages" not in st.session_state:
    st.session_state.messages = []

# Load data for status summary
raw_stats_data, _ = load_machine_stats()
yield_data = load_predicted_yield()

# Replace machine counters with alerts for yield quality
if not yield_data.empty:
    # Check if any machines have yield in warning or critical zones
    low_yield_machines = []
    critical_yield_machines = []
    
    for _, row in yield_data.iterrows():
        machine = row['MACHINE_NAME']
        yield_value = row['PREDICTED_YIELD']
        
        # Convert to percentage if necessary
        if 0 < yield_value < 5:
            yield_value = yield_value * 100
            
        # Check thresholds - matching gauge thresholds
        if yield_value < 92:  # Critical (red)
            critical_yield_machines.append((machine, yield_value))
        elif yield_value < 95:  # Warning (yellow)
            low_yield_machines.append((machine, yield_value))
    
    # Display alerts if needed - matching gauge colors
    if critical_yield_machines:
        st.markdown("""
        <div style="background-color:#FEE2E2; border-left:4px solid #CC0000; padding:15px; margin-bottom:20px; border-radius:4px;">
            <div style="display:flex; align-items:center;">
                <span style="color:#CC0000; font-size:20px; margin-right:10px;">⚠️</span>
                <div>
                    <h3 style="margin:0; color:#B91C1C; font-size:16px;">Critical Yield Alert</h3>
                    <p style="margin:5px 0 0 0; color:#7F1D1D;">
        """, unsafe_allow_html=True)
        
        for machine, value in critical_yield_machines:
            st.markdown(f"<span style='font-weight:500;'>{machine}</span>: {value:.2f}% (below critical threshold of 92%)<br>", unsafe_allow_html=True)
        
        st.markdown("""
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    if low_yield_machines:
        st.markdown("""
        <div style="background-color:#FEF3C7; border-left:4px solid #FFEB3B; padding:15px; margin-bottom:20px; border-radius:4px;">
            <div style="display:flex; align-items:center;">
                <span style="color:#D97706; font-size:20px; margin-right:10px;">⚠️</span>
                <div>
                    <h3 style="margin:0; color:#B45309; font-size:16px;">Yield Warning</h3>
                    <p style="margin:5px 0 0 0; color:#92400E;">
        """, unsafe_allow_html=True)
        
        for machine, value in low_yield_machines:
            st.markdown(f"<span style='font-weight:500;'>{machine}</span>: {value:.2f}% (below warning threshold of 95%)<br>", unsafe_allow_html=True)
        
        st.markdown("""
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
    # If no alerts, show a success message - matching gauge green
    if not critical_yield_machines and not low_yield_machines:
        st.markdown("""
        <div style="background-color:#ECFDF5; border-left:4px solid #008000; padding:15px; margin-bottom:20px; border-radius:4px;">
            <div style="display:flex; align-items:center;">
                <span style="color:#059669; font-size:20px; margin-right:10px;">✅</span>
                <div>
                    <h3 style="margin:0; color:#065F46; font-size:16px;">All Yields Normal</h3>
                    <p style="margin:5px 0 0 0; color:#065F46;">
                        All machines are operating with predicted yields in the normal range (95% and above).
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

# Display predicted yield gauges with improved layout
st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown('<h2 class="section-heading">Predicted Yield by Machine</h2>', unsafe_allow_html=True)

# Create a row of gauges using columns
if not yield_data.empty:
    # Create columns for gauge charts
    gauge_cols = st.columns(len(all_machines))
    
    # Create a gauge for each machine
    for i, machine in enumerate(all_machines):
        # Find the yield value for this machine
        machine_yield = yield_data[yield_data['MACHINE_NAME'] == machine]['PREDICTED_YIELD'].values
        
        # Use the first value if found, otherwise use 0
        yield_value = machine_yield[0] if len(machine_yield) > 0 else 0
        
        # Create and display the gauge in its column
        with gauge_cols[i % len(gauge_cols)]:
            st.plotly_chart(
                create_gauge_chart(machine, yield_value), 
                use_container_width=True
            )
else:
    st.warning("No predicted yield data available")

st.markdown('</div>', unsafe_allow_html=True)  # Close card

# Load sensor data with loading indicator
with st.spinner("Loading latest sensor data..."):
    data = load_sensor_data()

# Display status information with better styling
if not data.empty:
    st.markdown(f"""
    <div style="background-color:#eef2ff; padding:10px 15px; border-radius:4px; margin:15px 0; display:flex; justify-content:space-between; align-items:center;">
        <div>
            <span style="font-weight:500;">📊 Displaying data for {len(st.session_state.selected_machines)} machines</span>
            <span style="margin-left:10px; color:#6b7280; font-size:14px;">Time range: {data['TIMESTAMP'].min().strftime('%H:%M:%S')} to {data['TIMESTAMP'].max().strftime('%H:%M:%S')}</span>
        </div>
        <div class="last-updated">
            Updated {datetime.now().strftime('%H:%M:%S')}
        </div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.warning(f"No data available for the selected machines in the last {st.session_state.time_window_minutes} minutes")

# Create improved tabs
tab_titles = ["Sensor Dashboard", "Mill Machine Assistant"]
active_tab = st.radio("", tab_titles, horizontal=True, label_visibility="collapsed")
st.session_state.active_tab = active_tab

# Show content based on active tab
if st.session_state.active_tab == "Sensor Dashboard":
    # Content for Sensor Dashboard tab with improved styling
    with st.expander("Sensor Measurements Dashboard", expanded=True):
        # Show current machine selection for reference
        if len(st.session_state.selected_machines) < len(all_machines):
            machines_text = ", ".join(st.session_state.selected_machines)
            st.caption(f"Showing data for: {machines_text}")
        else:
            st.caption(f"Showing data for all {len(all_machines)} machines")
        
        # Use tabs for each chart to avoid duplication with enhanced styling
        tab1, tab2, tab3, tab4 = st.tabs([
            "Spindle Speed", 
            "Vibration", 
            "Feed Rate", 
            "Tool Wear"
        ])
        
        # Display charts in tabs with better titles and descriptions
        with tab1:
            st.markdown('<h3 class="section-heading">Spindle Speed</h3>', unsafe_allow_html=True)
            st.markdown("""
            <div style="margin-bottom:10px; font-size:14px; color:#666;">
                <span class="tooltip">ℹ️
                    <span class="tooltiptext">Nominal range for spindle speed is 2800-3200 RPM. Values outside this range may indicate issues.</span>
                </span>
                Monitoring real-time spindle speed across all machines. Nominal range: 2800-3200 RPM.
            </div>
            """, unsafe_allow_html=True)
            st.plotly_chart(create_chart(data, 'SPINDLE_SPEED'), use_container_width=True)
        
        with tab2:
            st.markdown('<h3 class="section-heading">Vibration</h3>', unsafe_allow_html=True)
            st.markdown("""
            <div style="margin-bottom:10px; font-size:14px; color:#666;">
                <span class="tooltip">ℹ️
                    <span class="tooltiptext">Excessive vibration can indicate mechanical issues or imbalance in the machine.</span>
                </span>
                Vibration levels for each machine. Nominal range: 0.1-0.3 units.
            </div>
            """, unsafe_allow_html=True)
            st.plotly_chart(create_chart(data, 'VIBRATION'), use_container_width=True)
        
        with tab3:
            st.markdown('<h3 class="section-heading">Feed Rate</h3>', unsafe_allow_html=True)
            st.markdown("""
            <div style="margin-bottom:10px; font-size:14px; color:#666;">
                <span class="tooltip">ℹ️
                    <span class="tooltiptext">Feed rate affects material quality and tool wear. Too fast can damage tools, too slow reduces efficiency.</span>
                </span>
                Material feed rate tracking. Nominal range: 100-120 units.
            </div>
            """, unsafe_allow_html=True)
            st.plotly_chart(create_chart(data, 'FEED_RATE'), use_container_width=True)
        
        with tab4:
            st.markdown('<h3 class="section-heading">Tool Wear</h3>', unsafe_allow_html=True)
            st.markdown("""
            <div style="margin-bottom:10px; font-size:14px; color:#666;">
                <span class="tooltip">ℹ️
                    <span class="tooltiptext">Tool wear above 70% may affect product quality. Tools should be replaced before reaching 80% wear.</span>
                </span>
                Tool wear percentage over time. Optimal range: 0-50%.
            </div>
            """, unsafe_allow_html=True)
            st.plotly_chart(create_chart(data, 'TOOL_WEAR'), use_container_width=True)
    
    # Load machine stats data with spinner
    with st.spinner("Loading machine statistics..."):
        raw_stats_data, html_table = load_machine_stats()
    
    # Create a collapsible section for machine stats table with better styling
    with st.expander("Machine Stats", expanded=True):
        # Add a more professional color legend
        st.markdown("""
        <div style="background-color:#f8f9fa; padding:12px; border-radius:4px; margin-bottom:15px;">
            <h4 style="margin-top:0; margin-bottom:10px;">Color Legend</h4>
            <div style="display:flex; flex-wrap:wrap; gap:8px; align-items:center;">
                <div style="display:flex; align-items:center; margin-right:16px;">
                    <div style="width:15px; height:15px; background-color:darkred; margin-right:6px; border-radius:2px;"></div>
                    <span style="font-size:14px;">Far Above Normal</span>
                </div>
                <div style="display:flex; align-items:center; margin-right:16px;">
                    <div style="width:15px; height:15px; background-color:red; margin-right:6px; border-radius:2px;"></div>
                    <span style="font-size:14px;">Above Normal</span>
                </div>
                <div style="display:flex; align-items:center; margin-right:16px;">
                    <div style="width:15px; height:15px; background-color:yellow; margin-right:6px; border-radius:2px;"></div>
                    <span style="font-size:14px;">Warning</span>
                </div>
                <div style="display:flex; align-items:center; margin-right:16px;">
                    <div style="width:15px; height:15px; background-color:blue; margin-right:6px; border-radius:2px;"></div>
                    <span style="font-size:14px;">Below Normal</span>
                </div>
                <div style="display:flex; align-items:center;">
                    <div style="width:15px; height:15px; background-color:darkblue; margin-right:6px; border-radius:2px;"></div>
                    <span style="font-size:14px;">Far Below Normal</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        if not raw_stats_data.empty:
            # Display the HTML table
            st.markdown(html_table, unsafe_allow_html=True)
            
            # Add export button - NEW FEATURE
            try:
                # Check if raw_stats_data is already a pandas DataFrame
                if isinstance(raw_stats_data, pd.DataFrame):
                    pandas_df = raw_stats_data
                else:
                    # Try to convert to pandas DataFrame if it's not already
                    pandas_df = raw_stats_data.to_pandas()
                
                # Now use pandas DataFrame's to_csv method
                st.download_button(
                    label="Export Stats to CSV",
                    data=pandas_df.to_csv(index=False).encode('utf-8'),
                    file_name=f"machine_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
            except Exception as e:
                st.warning(f"Could not prepare data for download: {str(e)}")
        else:
            st.warning("No machine stats data available")

else:  # Mill Machine Assistant tab is active
    # Enhanced Mill Machine Assistant interface
    st.markdown('<div class="card" style="padding:0;">', unsafe_allow_html=True)
    st.markdown("""
    <div style="background-color:#1f2937; color:white; padding:20px; border-radius:8px 8px 0 0;">
        <h2 style="margin:0; display:flex; align-items:center; color:white;">
            <span style="margin-right:10px;">🤖</span> Intelligent Mill Machine Assistant
        </h2>
        <p style="margin:10px 0 0 0; opacity:0.8; color:white;">
            Ask me anything about mill machine operation, maintenance, or troubleshooting.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # New chat button with better styling
    with st.sidebar:
        st.markdown("<h4>Assistant Controls</h4>", unsafe_allow_html=True)
        if st.button("New Conversation", key="new_chat", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
    
    # Create a container for the chat with better styling
    chat_container = st.container()
    
    # Display chat history with improved styling
    with chat_container:
        if not st.session_state.messages:
            # Simplified empty state message
            st.markdown("""
            <div style="text-align:center; padding:30px; color:#6b7280;">
                <p style="font-size:16px; margin-bottom:20px;">Ask a question about mill machines to get started</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            # Show existing messages with improved styling
            for message in st.session_state.messages:
                with st.chat_message(message['role']):
                    formatted_content = message['content'].replace("•", "\n\n")
                    st.markdown(formatted_content)
    
    # Chat input with better styling
    if query := st.chat_input("What would you like to know about mill machines?"):
        # Add user message to chat
        with st.chat_message("user"):
            st.markdown(query)
        st.session_state.messages.append({"role": "user", "content": query})
        
        # Get response from API with better loading experience
        with st.spinner("Processing your request..."):
            response = snowflake_api_call(query, 1)
            text, sql, citations = process_sse_response(response)
            
            # Add assistant response to chat with enhanced styling
            if text:
                text = text.replace("【†", "[")
                text = text.replace("†】", "]")
                
                # Store SQL in session state for this message
                if sql:
                    has_sql = True
                    message_id = len(st.session_state.messages)
                    if 'sql_data' not in st.session_state:
                        st.session_state.sql_data = {}
                    st.session_state.sql_data[message_id] = sql
                else:
                    has_sql = False
                
                # Add the complete response to chat history
                st.session_state.messages.append({"role": "assistant", "content": text})
                
                # Display the response with enhanced styling
                with st.chat_message("assistant"):
                    st.markdown(text.replace("•", "\n\n"))
                    
                    # If we have SQL, add an expander to show it optionally
                    if has_sql:
                        message_id = len(st.session_state.messages) - 1
                        with st.expander("View Generated SQL"):
                            st.code(st.session_state.sql_data[message_id], language="sql")
                            
                        # Run the SQL and get results with better styling
                        sales_results = run_snowflake_query(sql)
                        if sales_results is not None:
                            st.markdown("""
                            <h3 style="font-size:18px; margin-top:20px; margin-bottom:10px; padding-bottom:5px; border-bottom:1px solid #e5e7eb;">
                                Mill Metrics Report
                            </h3>
                            """, unsafe_allow_html=True)
                            st.dataframe(sales_results, use_container_width=True)
                            
                            # Convert to pandas dataframe first, then to CSV
                            try:
                                # First convert to pandas DataFrame
                                pandas_df = sales_results.to_pandas()
                                
                                # Now use pandas DataFrame's to_csv method
                                st.download_button(
                                    label="Download Results",
                                    data=pandas_df.to_csv(index=False).encode('utf-8'),
                                    file_name=f"mill_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv",
                                )
                            except Exception as e:
                                st.warning(f"Could not prepare data for download: {str(e)}")
    
    st.markdown('</div>', unsafe_allow_html=True)  # Close card

# Auto-refresh for the Sensor Dashboard tab only
if st.session_state.active_tab == "Sensor Dashboard" and enable_auto_refresh:
    # Show countdown
    st.markdown(f"""
    <div style="text-align:right; margin-top:20px; padding:5px 10px; background-color:#f8f9fa; border-radius:4px; display:inline-block; float:right;">
        <span style="font-size:12px; color:#6b7280;">
            <span style="margin-right:5px;">🔄</span>
            Auto-refreshing in {st.session_state.refresh_interval} seconds...
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Wait and then trigger refresh
    time.sleep(st.session_state.refresh_interval)
    st.rerun()