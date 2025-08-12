# =============================================================================
# 1. IMPORTS
# =============================================================================
import pandas as pd
import pydeck as pdk
import streamlit as st
import branca.colormap as cm
from snowflake.snowpark.context import get_active_session


# =============================================================================
# 2. CONFIGURATION & CONSTANTS
# =============================================================================
st.set_page_config(layout="centered", initial_sidebar_state="expanded")
COLOR_PALETTE = ["gray", "blue", "green", "yellow", "orange", "red"]
INITIAL_VIEW_STATE = pdk.ViewState(
    latitude=37.633, longitude=-122.284, zoom=7, pitch=50
)


# =============================================================================
# 3. DATA & HELPER FUNCTIONS
# =============================================================================
@st.cache_data
def get_dataframe_from_sql(query: str) -> pd.DataFrame:
    """Executes a SQL query against Snowflake and returns a Pandas DataFrame."""
    session = get_active_session()
    return session.sql(query).to_pandas()


def fetch_h3_data(
    resolution: int, dimension: str, measure: str, score_range: tuple = None
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Fetches and processes H3 hexagon data based on user selections.

    This function dynamically builds a SQL query to aggregate either order counts
    or average sentiment scores, grouped by H3 cell.
    """
    # Determine the aggregation logic and WHERE clause based on the selected measure
    if measure == "ORDERS":
        aggregation_sql = "ROUND(COUNT(*), 2) AS count"
        where_clause = ""
    else:
        aggregation_sql = f"ROUND(AVG({measure}), 2) AS count"
        where_clause = f"WHERE {measure} IS NOT NULL"

    # Build the full SQL query using an f-string for readability
    query = f"""
        SELECT
            H3_POINT_TO_CELL_STRING(TO_GEOGRAPHY({dimension}), {resolution}) AS h3,
            {aggregation_sql}
        FROM
            advanced_analytics.public.orders_reviews_sentiment_analysis
        {where_clause}
        GROUP BY 1
    """
    df = get_dataframe_from_sql(query)
    if df.empty:
        return pd.DataFrame(columns=["h3", "COUNT"]), pd.Series()

    # Calculate quantiles before any filtering
    quantiles = df["COUNT"].quantile([0, 0.25, 0.5, 0.75, 1])

    # Filter the DataFrame by the selected score range if applicable
    if score_range:
        df = df[(df["COUNT"] >= score_range[0]) & (df["COUNT"] <= score_range[1])]

    return df, quantiles


# =============================================================================
# 4. UI & CHARTING FUNCTIONS
# =============================================================================
def render_sidebar() -> tuple:
    """Renders the sidebar controls and returns user selections."""
    with st.sidebar:
        st.header("🗺️ Map Controls")
        h3_resolution = st.slider(
            "Hexagon Resolution (Higher is smaller)", min_value=6, max_value=9, value=7
        )

        selected_dimension = st.selectbox(
            "Location Type:", ("DELIVERY_LOCATION", "RESTAURANT_LOCATION")
        )

        selected_measure = st.selectbox(
            "Metric to Display:",
            ("ORDERS", "SENTIMENT_SCORE", "COST_SCORE", "FOOD_QUALITY_SCORE", "DELIVERY_TIME_SCORE"),
        )

        # Conditionally show the score range slider or the 3D checkbox
        score_range = None
        is_3d_view = False
        if selected_measure != "ORDERS":
            score_range = st.slider(
                "Filter by Score Range:", 0.0, 5.0, (0.0, 5.0)
            )
        else:
            is_3d_view = st.checkbox("Show 3D View", help="Render H3 hexagons in 3D")

    return h3_resolution, selected_dimension, selected_measure, score_range, is_3d_view


def render_pydeck_chart(
    df: pd.DataFrame, quantiles: pd.Series, is_3d_view: bool = False
):
    """Creates and displays the Pydeck map chart."""
    if df is None or df.empty:
        st.warning("No data available for the selected filters.")
        return

    # Create a color map and apply it to the DataFrame
    color_map = cm.LinearColormap(
        COLOR_PALETTE,
        vmin=quantiles.min(),
        vmax=quantiles.max(),
        index=quantiles,
    )
    df["COLOR"] = df["COUNT"].apply(color_map.rgb_bytes_tuple)

    # Define the H3 hexagon layer
    h3_layer = pdk.Layer(
        "H3HexagonLayer",
        data=df,
        get_hexagon="H3",
        get_fill_color="COLOR",
        get_line_color="COLOR",
        auto_highlight=True,
        get_elevation="COUNT / " + str(df["COUNT"].max()) if not df.empty else "0",
        elevation_scale=10000 if is_3d_view else 0,
        pickable=True,
        extruded=True,
    )

    # Set the correct pitch for 2D vs 3D view
    view_state = INITIAL_VIEW_STATE
    view_state.pitch = 50 if is_3d_view else 0

    st.image(
        "https://sfquickstarts.s3.us-west-1.amazonaws.com/hol_geo_spatial_ml_using_snowflake_cortex/gradient.png"
    )

    st.pydeck_chart(
        pdk.Deck(
            map_style="light",# This gives you a light theme
            initial_view_state=view_state,
            tooltip={"html": "<b>Value:</b> {COUNT}", "style": {"color": "white"}},
            layers=[h3_layer],
        )
    )


# =============================================================================
# 5. MAIN APPLICATION
# =============================================================================
def main():
    """Main function to run the Streamlit application."""
    st.title("Food Delivery Order & Sentiment Analysis 🍔")

    # 1. Get user inputs from the sidebar
    resolution, dimension, measure, score_range, is_3d = render_sidebar()

    # 2. Fetch data based on inputs
    df, quantiles = fetch_h3_data(resolution, dimension, measure, score_range)

    # 3. Render the final map
    render_pydeck_chart(df, quantiles, is_3d)


if __name__ == "__main__":
    main()
