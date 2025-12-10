import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import duckdb
import plotly.express as px
import pandas as pd

# --- Configuration ---
DB_FILE = "cycling.duckdb"

def get_db_connection():
    """Return a read-only DuckDB connection."""
    return duckdb.connect(DB_FILE, read_only=True)

# --- Initial Date Range from raw table ---
con = get_db_connection()
try:
    # Use start_time instead of start_date for NYC schema
    min_date, max_date = con.sql(
        "SELECT MIN(start_time), MAX(start_time) FROM nyc_biking_data"
    ).fetchone()
finally:
    con.close()

app = dash.Dash(__name__)

# --- Layout ---
app.layout = html.Div(
    [
        # Header
        html.Div(
            [
                html.H1(
                    "NYC Citi Bike Dashboard",
                    style={"margin": "0", "color": "#2c3e50"},
                ),
                html.P(
                    "Historical ride data analysis (2018–2023)",
                    style={"color": "#7f8c8d", "margin": "4px 0 0 0"},
                ),
                html.Button(
                    "Download PDF",
                    id="print-btn",
                    n_clicks=0,
                    style={
                        "marginTop": "10px",
                        "padding": "8px 16px",
                        "backgroundColor": "#2c3e50",
                        "color": "white",
                        "border": "none",
                        "borderRadius": "5px",
                        "cursor": "pointer",
                        "fontSize": "13px",
                    },
                ),
            ],
            style={
                "textAlign": "center",
                "padding": "20px",
                "backgroundColor": "#ecf0f1",
                "marginBottom": "16px",
            },
        ),
        # Controls (inline, consistent labeling)
        html.Div(
            [
                html.Div(
                    [
                        html.Label(
                            "Select Date Range:",
                            style={
                                "fontWeight": "bold",
                                "marginBottom": "6px",
                                "display": "block",
                            },
                        ),
                        dcc.DatePickerRange(
                            id="date-picker-range",
                            min_date_allowed=min_date,
                            max_date_allowed=max_date,
                            start_date=min_date,
                            end_date=max_date,
                            display_format="YYYY-MM-DD",
                            style={
                                "border": "none",
                                "padding": "0",
                                "height": "44px",
                            },
                        ),
                    ],
                    style={"minWidth": "260px"},
                ),
                html.Div(
                    [
                        html.Label(
                            "User Type:",
                            style={
                                "fontWeight": "bold",
                                "marginBottom": "6px",
                                "display": "block",
                            },
                        ),
                        dcc.Dropdown(
                            id="user-type-dropdown",
                            options=[
                                {"label": "All", "value": "All"},
                                {"label": "Member", "value": "member"},
                                {"label": "Casual", "value": "casual"},
                            ],
                            value="All",
                            clearable=False,
                            style={
                                "width": "240px",
                                "height": "44px",
                                "padding": "6px 10px",
                                "border": "1px solid #ccc",
                                "borderRadius": "5px",
                            },
                        ),
                    ],
                    style={"minWidth": "260px"},
                ),
            ],
            style={
                "padding": "0 24px 12px 24px",
                "display": "flex",
                "justifyContent": "center",
                "alignItems": "center",
                "gap": "16px",
                "flexWrap": "wrap",
            },
        ),
        # Charts
        dcc.Loading(
            id="loading-1",
            type="circle",
            children=[
                # Row 1: Total Rides
                html.Div(
                    [
                        dcc.Graph(
                            id="rides-chart",
                            style={
                                "boxShadow": "0 4px 8px 0 rgba(0,0,0,0.2)",
                                "marginBottom": "20px",
                            },
                        )
                    ],
                    style={"padding": "0 40px"},
                ),
                # Row 2: Durations
                html.Div(
                    [
                        html.Div(
                            [
                                dcc.Graph(
                                    id="avg-duration-chart",
                                    style={
                                        "boxShadow": "0 4px 8px 0 rgba(0,0,0,0.2)"
                                    },
                                )
                            ],
                            style={
                                "width": "48%",
                                "display": "inline-block",
                                "marginRight": "2%",
                            },
                        ),
                        html.Div(
                            [
                                dcc.Graph(
                                    id="total-duration-chart",
                                    style={
                                        "boxShadow": "0 4px 8px 0 rgba(0,0,0,0.2)"
                                    },
                                )
                            ],
                            style={
                                "width": "48%",
                                "display": "inline-block",
                                "verticalAlign": "top",
                            },
                        ),
                    ],
                    style={"padding": "0 40px", "marginBottom": "140px"},
                ),
                # Row 3: Stations
                html.Div(
                    [
                        html.Div(
                            [
                                dcc.Graph(
                                    id="start-station-chart",
                                    style={
                                        "boxShadow": "0 4px 8px 0 rgba(0,0,0,0.2)"
                                    },
                                )
                            ],
                            style={
                                "width": "48%",
                                "display": "inline-block",
                                "marginRight": "2%",
                            },
                        ),
                        html.Div(
                            [
                                dcc.Graph(
                                    id="end-station-chart",
                                    style={
                                        "boxShadow": "0 4px 8px 0 rgba(0,0,0,0.2)"
                                    },
                                )
                            ],
                            style={
                                "width": "48%",
                                "display": "inline-block",
                                "verticalAlign": "top",
                            },
                        ),
                    ],
                    style={"padding": "0 40px", "marginBottom": "20px"},
                ),
                # Row 4: Day & Hour
                html.Div(
                    [
                        html.Div(
                            [
                                dcc.Graph(
                                    id="day-of-week-chart",
                                    style={
                                        "boxShadow": "0 4px 8px 0 rgba(0,0,0,0.2)",
                                        "marginBottom": "20px",
                                    },
                                )
                            ],
                            style={
                                "width": "48%",
                                "display": "inline-block",
                                "marginRight": "2%",
                            },
                        ),
                        html.Div(
                            [
                                dcc.Graph(
                                    id="hourly-chart",
                                    style={
                                        "boxShadow": "0 4px 8px 0 rgba(0,0,0,0.2)",
                                        "marginBottom": "20px",
                                    },
                                )
                            ],
                            style={
                                "width": "48%",
                                "display": "inline-block",
                                "verticalAlign": "top",
                            },
                        ),
                    ],
                    style={"padding": "0 40px", "marginBottom": "140px"},
                ),
            # Row 5: Geospatial Heatmap (Full Width)
            html.Div(
                [
                    dcc.Graph(
                        id="map-chart",
                        style={
                            "boxShadow": "0 4px 8px 0 rgba(0,0,0,0.2)",
                            "marginBottom": "40px",
                        },
                    )
                ],
                style={"padding": "0 40px"},
            ),
            # Row 6: Top Routes
            html.Div(
                [
                    dcc.Graph(
                        id="routes-chart",
                        style={
                            "boxShadow": "0 4px 8px 0 rgba(0,0,0,0.2)",
                            "marginBottom": "32px",
                        },
                    )
                ],
                style={"padding": "0 40px"},
            ),
        ],
        style={"padding": "0 40px"},
    ),
        # hidden div used for clientside print trigger
        html.Div(id="print-trigger", style={"display": "none"}),
    ],
    style={
        "fontFamily": "Helvetica, Arial, sans-serif",
        "backgroundColor": "#f9f9f9",
        "minHeight": "100vh",
    },
)


@app.callback(
    [
        Output("rides-chart", "figure"),
        Output("avg-duration-chart", "figure"),
        Output("total-duration-chart", "figure"),
        Output("start-station-chart", "figure"),
        Output("end-station-chart", "figure"),
        Output("day-of-week-chart", "figure"),
        Output("hourly-chart", "figure"),
        Output("map-chart", "figure"),
        Output("routes-chart", "figure"),
    ],
    [
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("user-type-dropdown", "value"),
    ],
)
def update_charts(start_date, end_date, user_type):
    if not start_date or not end_date:
        empty = px.line(title="Select a date range")
        return (empty,) * 10

    # Build user_type filter
    if user_type and user_type.lower() != "all":
        ut_filter = f"AND lower(user_type) = '{user_type.lower()}'"
    else:
        ut_filter = ""

    con = get_db_connection()
    try:
        # Time series
        # Note: Calculate duration on the fly as (end_time - start_time) in seconds
        df_ts = con.sql(
            f"""
            SELECT 
                date_trunc('month', start_time) AS start_month,
                count(*) AS total_rides,
                avg(date_diff('second', start_time, end_time)) / 60.0 AS avg_duration_minutes,
                sum(date_diff('second', start_time, end_time)) / 86400.0 AS total_duration_days
            FROM nyc_biking_data
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
            {ut_filter}
            GROUP BY 1
            ORDER BY 1
        """
        ).df()

        # Top start stations
        df_start = con.sql(
            f"""
            SELECT start_station_name, COUNT(*) AS rides
            FROM nyc_biking_data
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
              {ut_filter}
              AND start_station_name IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 10
        """
        ).df()

        # Top end stations
        df_end = con.sql(
            f"""
            SELECT end_station_name, COUNT(*) AS rides
            FROM nyc_biking_data
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
              {ut_filter}
              AND end_station_name IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 10
        """
        ).df()

        # Day of week
        df_dow = con.sql(
            f"""
            SELECT 
                dayname(start_time) AS day_name,
                isodow(start_time) AS day_index,
                COUNT(*) AS rides
            FROM nyc_biking_data
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
              {ut_filter}
            GROUP BY 1, 2
            ORDER BY 2
        """
        ).df()

        # Hour of day
        df_hour = con.sql(
            f"""
            SELECT 
                hour(start_time) AS hour_of_day,
                COUNT(*) AS rides
            FROM nyc_biking_data
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
              {ut_filter}
            GROUP BY 1
            ORDER BY 1
        """
        ).df()

        # Geospatial Map Data (Start Locations)
        # Round lat/lng to group nearby points (3 decimals ~100m)
        df_map = con.sql(
            f"""
            SELECT 
                ROUND(start_lat, 3) as lat, 
                ROUND(start_lng, 3) as lng, 
                COUNT(*) as rides 
            FROM nyc_biking_data 
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
              {ut_filter}
              AND start_lat IS NOT NULL AND start_lng IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT 5000  -- Limit points for performance
        """
        ).df()

        df_routes = con.sql(
            f"""
            SELECT 
                CONCAT(start_station_name, ' \u2192 ', end_station_name) AS route,
                COUNT(*) AS rides
            FROM nyc_biking_data
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
              {ut_filter}
              AND start_station_name IS NOT NULL
              AND end_station_name IS NOT NULL
            GROUP BY 1
            ORDER BY rides DESC
            LIMIT 10
        """
        ).df()

    finally:
        con.close()

    if df_ts.empty:
        empty = px.line(title="No data found")
        return (empty,) * 9

    template = "plotly_white"

    # Time series figures
    fig_rides = px.line(
        df_ts,
        x="start_month",
        y="total_rides",
        title="Total Rides per Month",
        template=template,
    )
    fig_rides.update_traces(line_color="#3498db", line_width=3)
    fig_rides.update_layout(height=400, margin=dict(l=40, b=40, t=40, r=10))

    fig_avg = px.line(
        df_ts,
        x="start_month",
        y="avg_duration_minutes",
        title="Average Ride Duration (minutes)",
        template=template,
    )
    fig_avg.update_traces(line_color="#e74c3c", line_width=2)
    fig_avg.update_layout(height=350, margin=dict(l=40, b=40, t=40, r=10), yaxis_title="Minutes")

    fig_total = px.line(
        df_ts,
        x="start_month",
        y="total_duration_days",
        title="Total Time Cycled (days)",
        template=template,
    )
    fig_total.update_traces(line_color="#27ae60", line_width=2)
    fig_total.update_layout(height=350, margin=dict(l=40, b=40, t=40, r=10), yaxis_title="Days")

    # Station charts
    fig_start = px.bar(
        df_start,
        x="rides",
        y="start_station_name",
        orientation="h",
        title="Top 10 Start Locations",
        template=template,
    )
    fig_start.update_traces(marker_color="#8e44ad")
    fig_start.update_layout(
        height=400,
        margin=dict(l=10, b=40, t=40, r=10),
        yaxis=dict(categoryorder="total ascending"),
    )

    fig_end = px.bar(
        df_end,
        x="rides",
        y="end_station_name",
        orientation="h",
        title="Top 10 End Locations",
        template=template,
    )
    fig_end.update_traces(marker_color="#d35400")
    fig_end.update_layout(
        height=400,
        margin=dict(l=10, b=40, t=40, r=10),
        yaxis=dict(categoryorder="total ascending"),
    )

    # Temporal charts
    fig_dow = px.bar(
        df_dow,
        x="day_name",
        y="rides",
        title="Rides by Day of Week",
        template=template,
    )
    fig_dow.update_traces(marker_color="#f39c12")
    fig_dow.update_layout(height=400, margin=dict(l=40, b=40, t=40, r=10))

    fig_hour = px.bar(
        df_hour,
        x="hour_of_day",
        y="rides",
        title="Rides by Hour of Day",
        template=template,
    )
    fig_hour.update_traces(marker_color="#16a085")
    fig_hour.update_layout(height=400, margin=dict(l=40, b=40, t=40, r=10))

    # Map chart (density mapbox on start locations)
    if df_map.empty:
        fig_map = px.scatter_mapbox(
            pd.DataFrame({"lat": [], "lng": []}),
            lat="lat",
            lon="lng",
            title="NYC Start Station Density (no data)",
            zoom=9,
        )
        fig_map.update_layout(mapbox_style="carto-positron", height=500)
    else:
        fig_map = px.density_mapbox(
            df_map,
            lat="lat",
            lon="lng",
            z="rides",
            radius=12,
            center=dict(lat=40.7128, lon=-74.0060),
            zoom=10,
            title="NYC Start Station Density",
            mapbox_style="carto-positron",
        )
        fig_map.update_layout(height=500, margin=dict(l=40, b=40, t=40, r=10))

    if df_routes.empty:
        fig_routes = px.bar(title="Top Routes (no data)")
    else:
        fig_routes = px.bar(
            df_routes,
            x="rides",
            y="route",
            orientation="h",
            title="Top 10 Routes",
            template=template,
            color_discrete_sequence=["#9b59b6"],
        )
        fig_routes.update_layout(yaxis={"categoryorder": "total ascending"})

    return (
        fig_rides,
        fig_avg,
        fig_total,
        fig_start,
        fig_end,
        fig_dow,
        fig_hour,
        fig_map,
        fig_routes,
    )


# --- Clientside callback to trigger print dialog for PDF download ---
app.clientside_callback(
    """
    function(n_clicks) {
        if (!n_clicks) {
            return window.dash_clientside.no_update;
        }
        window.print();
        return "";
    }
    """,
    Output("print-trigger", "children"),
    Input("print-btn", "n_clicks"),
)


if __name__ == "__main__":
    app.run(debug=True, port=8051)
