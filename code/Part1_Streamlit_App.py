# ==============================================================================
# 1. IMPORTS
# ==============================================================================
import datetime
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
import branca.colormap as cm
from snowflake.snowpark.context import get_active_session


# ==============================================================================
# 2. CONFIGURATION & CONSTANTS
# ==============================================================================
# --- Page Config ---
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded",
    page_title="NY Taxi Pickup Analysis"
)

# --- SQL Queries ---
# Using f-strings with placeholders for dynamic parts
BASE_QUERY_TEMPLATE = """
    SELECT {h3_column}, {agg_function}({metric}) AS COUNT
    FROM advanced_analytics.public.ny_taxi_rides_compare
    WHERE
        pickup_time BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND TIME(pickup_time) BETWEEN '{start_time}' AND '{end_time}'
    GROUP BY 1
"""

TIME_SERIES_QUERY = "SELECT pickup_time, h3, forecast, pickups FROM advanced_analytics.public.ny_taxi_rides_compare"
METRICS_QUERY = "SELECT * FROM advanced_analytics.public.ny_taxi_rides_metrics"
AVG_COORDINATE_QUERY = "SELECT AVG(ST_Y(H3_CELL_TO_POINT(h3))) AS lat, AVG(ST_X(h3_cell_to_point(h3))) AS lon FROM advanced_analytics.public.ny_taxi_rides_compare"

# --- Constants ---
COLOR_PALETTE = ["gray", "blue", "green", "yellow", "orange", "red"]


# ==============================================================================
# 3. DATA & HELPER FUNCTIONS
# ==============================================================================
@st.cache_data
def load_data(query: str) -> pd.DataFrame:
    """Executes a SQL query and returns a pandas DataFrame."""
    session = get_active_session()
    return session.sql(query).to_pandas()

def generate_color_map(df: pd.DataFrame, count_col: str = "COUNT") -> pd.DataFrame:
    """
    Generates quantiles and applies a color map to the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame with a count column.
        count_col (str): The name of the column containing counts.

    Returns:
        pd.DataFrame: DataFrame with an added 'COLOR' column.
    """
    if df is None or df.empty:
        return df

    quantiles = df[count_col].quantile([0, 0.25, 0.5, 0.75, 1])
    linear_map = cm.LinearColormap(
        COLOR_PALETTE,
        vmin=quantiles.min(),
        vmax=quantiles.max(),
        index=quantiles
    )
    df["COLOR"] = df[count_col].apply(linear_map.rgb_bytes_tuple)
    return df


# ==============================================================================
# 4. UI & CHARTING FUNCTIONS
# ==============================================================================
def render_sidebar(metrics_df: pd.DataFrame) -> tuple:
    """Renders the sidebar and returns user selections."""
    with st.sidebar:
        st.header("Filters")

        # --- Date and Time Selection ---
        initial_start_date = datetime.date(2015, 6, 6)
        selected_date_range = st.date_input(
            "Date Range:",
            (initial_start_date, initial_start_date + datetime.timedelta(days=7)),
            format="MM.DD.YYYY",
        )

        time_col1, time_col2 = st.columns(2)
        with time_col1:
            start_time = st.time_input("Start Time", datetime.time(0, 0), step=3600)
        with time_col2:
            end_time = st.time_input("End Time", datetime.time(23, 0), step=3600)

        # --- H3 Cell Selection ---
        h3_options = ["All"] + metrics_df["H3"].to_list()
        selected_h3 = st.selectbox("H3 Cell to Display:", h3_options)

        # --- Metrics Expander ---
        with st.expander("Expand to see SMAPE metric"):
            df_to_display = metrics_df
            if selected_h3 != "All":
                df_to_display = metrics_df[metrics_df["H3"] == selected_h3]
            st.dataframe(df_to_display, hide_index=True, width=300)

        # --- 3D View Checkbox ---
        is_3d_view = st.checkbox("Show 3D View", help="Render H3 Hexagons in 3D")

    return selected_date_range, (start_time, end_time), selected_h3, is_3d_view

def render_pydeck_chart(chart_df: pd.DataFrame, coordinates: tuple, elevation_3d: bool):
    """Renders a Pydeck map chart."""
    if chart_df is None or chart_df.empty:
        st.warning("No data to display for the selected filters.")
        return

    max_count = chart_df["COUNT"].max()
    st.image('https://sfquickstarts.s3.us-west-1.amazonaws.com/hol_geo_spatial_ml_using_snowflake_cortex/gradient.png')

    st.pydeck_chart(pdk.Deck(
        map_style=None,
        initial_view_state=pdk.ViewState(
            latitude=coordinates[0], longitude=coordinates[1], pitch=45, zoom=10
        ),
        tooltip={"html": "<b>{H3}:</b> {COUNT}", "style": {"color": "white"}},
        layers=[pdk.Layer(
            "H3HexagonLayer",
            data=chart_df,
            get_hexagon="H3",
            get_fill_color="COLOR",
            get_line_color="COLOR",
            get_elevation=f"COUNT / {max_count}" if max_count > 0 else 0,
            auto_highlight=True,
            elevation_scale=10000 if elevation_3d else 0,
            pickable=True,
            extruded=True,
            opacity=0.3,
        )],
    ))

def render_plotly_line_chart(chart_df: pd.DataFrame):
    """Renders a Plotly line chart for time series comparison."""
    fig = px.line(
        chart_df,
        x="PICKUP_TIME",
        y=["PICKUPS", "FORECAST"],
        color_discrete_sequence=["#D966FF", "#126481"],
        markers=True,
        labels={"value": "Number of Rides", "variable": "Metric"}
    )
    fig.update_layout(yaxis_title="Pickups", xaxis_title="")
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)


# ==============================================================================
# 5. MAIN APPLICATION LOGIC
# ==============================================================================
def main():
    """Main function to run the Streamlit app."""
    # --- Page Title and Description ---
    st.title("NY Pickup Location App :balloon:")
    st.write("""
        An app that visualizes geo-temporal data from NY taxi pickups using H3 and time series analysis.
        It can be useful to visualize marketplace signals that are distributed spatially and temporally.
    """)

    # --- Initial Data Loading ---
    df_metrics = load_data(METRICS_QUERY)
    df_avg_lat_long = load_data(AVG_COORDINATE_QUERY)
    avg_coordinate = (df_avg_lat_long.iloc[0, 0], df_avg_lat_long.iloc[0, 1])

    # --- Sidebar and User Inputs ---
    date_range, time_range, selected_h3, is_3d = render_sidebar(df_metrics)

    # --- Main Content ---
    if len(date_range) != 2:
        st.warning("Please select a valid date range in the sidebar.")
        st.stop()

    start_date, end_date = date_range
    start_time, end_time = time_range

    # --- Data Loading and Processing for Maps ---
    # Build queries dynamically
    pickups_query = BASE_QUERY_TEMPLATE.format(h3_column='h3', agg_function='SUM', metric='pickups', start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time)
    forecast_query = BASE_QUERY_TEMPLATE.format(h3_column='h3', agg_function='SUM', metric='forecast', start_date=start_date, end_date=end_date, start_time=start_time, end_time=end_time)

    # Load and process data
    df_pickups = generate_color_map(load_data(pickups_query))
    df_forecast = generate_color_map(load_data(forecast_query))

    # Filter by H3 if selected
    if selected_h3 != "All":
        df_pickups = df_pickups[df_pickups["H3"] == selected_h3]
        df_forecast = df_forecast[df_forecast["H3"] == selected_h3]

    # --- Display Hexagon Maps ---
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Actual Demand")
        render_pydeck_chart(df_pickups, avg_coordinate, is_3d)
    with col2:
        st.subheader("Forecasted Demand")
        render_pydeck_chart(df_forecast, avg_coordinate, is_3d)

    st.divider()

    # --- Time Series Analysis ---
    st.subheader(f"Time Series Comparison: {'All Hexagons' if selected_h3 == 'All' else selected_h3}")

    df_time_series = load_data(TIME_SERIES_QUERY)
    df_time_series["PICKUP_TIME"] = pd.to_datetime(df_time_series["PICKUP_TIME"])

    # Apply filters
    is_in_date_range = (df_time_series["PICKUP_TIME"].dt.date >= start_date) & (df_time_series["PICKUP_TIME"].dt.date <= end_date)
    is_in_time_range = (df_time_series["PICKUP_TIME"].dt.time >= start_time) & (df_time_series["PICKUP_TIME"].dt.time <= end_time)
    
    df_filtered = df_time_series[is_in_date_range & is_in_time_range]

    if selected_h3 == "All":
        df_agg = df_filtered.groupby("PICKUP_TIME", as_index=False)[["PICKUPS", "FORECAST"]].sum()
    else:
        is_selected_h3 = (df_filtered["H3"] == selected_h3)
        df_agg = df_filtered[is_selected_h3].groupby("PICKUP_TIME", as_index=False)[["PICKUPS", "FORECAST"]].sum()
    
    if df_agg.empty:
        st.warning("No time series data available for the selected filters.")
    else:
        render_plotly_line_chart(df_agg)
        with st.expander("Show Raw Time Series Data"):
            st.dataframe(df_agg, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
