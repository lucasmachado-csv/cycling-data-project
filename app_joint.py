import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import duckdb
import plotly.express as px

DB_FILE = "cycling.duckdb"


def get_db_connection():
    return duckdb.connect(DB_FILE, read_only=True)


# Get date range and city list from joint table
con = get_db_connection()
try:
    min_date, max_date = con.sql(
        "SELECT MIN(start_time), MAX(start_time) FROM joint_bike_data"
    ).fetchone()
    city_options = [
        {"label": c, "value": c}
        for c, in con.sql("SELECT DISTINCT city FROM joint_bike_data ORDER BY city").fetchall()
    ]
finally:
    con.close()

app = dash.Dash(__name__)

app.layout = html.Div(
    [
        html.Div(
            [
                html.H1("Combined Cities Dashboard", style={"margin": "0", "color": "#2c3e50"}),
                html.P("London vs NYC ride metrics", style={"color": "#7f8c8d", "margin": "4px 0 0 0"}),
            ],
            style={
                "textAlign": "center",
                "padding": "18px",
                "backgroundColor": "#ecf0f1",
                "marginBottom": "12px",
            },
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Label(
                            "Select Date Range:",
                            style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"},
                        ),
                        dcc.DatePickerRange(
                            id="date-picker",
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
                            "Cities:",
                            style={"fontWeight": "bold", "marginBottom": "6px", "display": "block"},
                        ),
                        dcc.Dropdown(
                            id="city-dropdown",
                            options=city_options,
                            value=[opt["value"] for opt in city_options],
                            multi=True,
                            clearable=False,
                            style={
                                "width": "320px",
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
        dcc.Loading(
            type="circle",
            children=[
                html.Div(
                    [
                        dcc.Graph(id="rides-month-chart"),
                        dcc.Graph(id="avg-duration-chart"),
                        dcc.Graph(id="total-duration-chart"),
                    ],
                    style={"padding": "0 32px"},
                ),
                html.Div(
                    [
                        html.Div([dcc.Graph(id="dow-chart")], style={"width": "48%", "display": "inline-block", "marginRight": "2%"}),
                        html.Div([dcc.Graph(id="hour-chart")], style={"width": "48%", "display": "inline-block"}),
                    ],
                    style={"padding": "0 32px 24px 32px"},
                ),
            ],
        ),
    ],
    style={"fontFamily": "Helvetica, Arial, sans-serif", "backgroundColor": "#f9f9f9"},
)


@app.callback(
    [
        Output("rides-month-chart", "figure"),
        Output("avg-duration-chart", "figure"),
        Output("total-duration-chart", "figure"),
        Output("dow-chart", "figure"),
        Output("hour-chart", "figure"),
    ],
    [
        Input("date-picker", "start_date"),
        Input("date-picker", "end_date"),
        Input("city-dropdown", "value"),
    ],
)
def update_charts(start_date, end_date, cities):
    if not start_date or not end_date or not cities:
        empty = px.line(title="Select a date range and at least one city")
        return empty, empty, empty, empty, empty

    city_list = "', '".join([c.replace("'", "''") for c in cities])
    city_filter = f"AND city IN ('{city_list}')" if cities else ""

    con = get_db_connection()
    try:
        # Time series by month and city
        df_month = con.sql(
            f"""
            SELECT 
                date_trunc('month', start_time) AS month,
                city,
                count(*) AS rides,
                avg(duration_seconds) / 60.0 AS avg_duration_minutes,
                sum(duration_seconds) / 86400.0 AS total_duration_days
            FROM joint_bike_data
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
            {city_filter}
            GROUP BY 1, 2
            ORDER BY 1, 2
            """
        ).df()

        # Day of week
        df_dow = con.sql(
            f"""
            SELECT 
                dayname(start_time) AS day_name,
                isodow(start_time) AS day_index,
                city,
                count(*) AS rides
            FROM joint_bike_data
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
            {city_filter}
            GROUP BY 1, 2, 3
            ORDER BY day_index, city
            """
        ).df()

        # Hour of day
        df_hour = con.sql(
            f"""
            SELECT 
                hour(start_time) AS hour_of_day,
                city,
                count(*) AS rides
            FROM joint_bike_data
            WHERE start_time BETWEEN '{start_date}' AND '{end_date}'
            {city_filter}
            GROUP BY 1, 2
            ORDER BY 1, 2
            """
        ).df()
    finally:
        con.close()

    if df_month.empty:
        empty = px.line(title="No data for selection")
        return empty, empty, empty, empty, empty

    fig_rides = px.line(
        df_month,
        x="month",
        y="rides",
        color="city",
        title="Total Rides per Month",
        template="plotly_white",
    )
    fig_rides.update_traces(line_width=3)

    fig_avg = px.line(
        df_month,
        x="month",
        y="avg_duration_minutes",
        color="city",
        title="Average Ride Duration (minutes)",
        template="plotly_white",
    )

    fig_total = px.line(
        df_month,
        x="month",
        y="total_duration_days",
        color="city",
        title="Total Time Cycled (days)",
        template="plotly_white",
    )

    fig_dow = px.bar(
        df_dow,
        x="day_name",
        y="rides",
        color="city",
        title="Rides by Day of Week",
        template="plotly_white",
    )
    fig_dow.update_layout(xaxis=dict(categoryorder="array", categoryarray=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]))

    fig_hour = px.bar(
        df_hour,
        x="hour_of_day",
        y="rides",
        color="city",
        title="Rides by Hour of Day",
        template="plotly_white",
    )

    return fig_rides, fig_avg, fig_total, fig_dow, fig_hour


if __name__ == "__main__":
    app.run(debug=True, port=8052)


