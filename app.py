from io import StringIO
from pathlib import Path

import dash
from dash import Dash, dcc, html, Input, Output, State, dash_table
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

import functions as fx

# -------- Data load / initial state --------
RAW = fx.load_data()
# If you later enrich with a registry CSV, RAW may include county/sub_county
FILTER_META = fx.available_filters(RAW)  # vendors, months, years
INITIAL_JSON = RAW.to_json(date_format="iso", orient="split")

app = Dash(__name__, title="SHA Disbursements")
server = app.server  # for WSGI deploys


def stat_card(title, value, id_val=None):
    return html.Div(
        className="card",
        children=[
            html.Div(title, className="card-title"),
            html.Div(
                f"{value:,}" if isinstance(value, (int, float)) else value,
                className="card-value",
                id=id_val,
            ),
        ],
    )


# No dash-table Format usage (keeps compatibility across versions)
amount_col = {"name": "amount", "id": "amount", "type": "numeric"}


def make_month_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add chronological month key columns if report_year & report_month exist.
    - 'ym' (Period[M]) for sorting
    - 'year_month' display label like 'Apr-2025'
    """
    if {"report_year", "report_month"}.issubset(df.columns):
        dt = pd.to_datetime(
            df["report_year"].astype(str) + "-" + df["report_month"].astype(str),
            format="%Y-%B",
            errors="coerce",
        )
        df = df.assign(
            ym=dt.dt.to_period("M"),
            year_month=dt.dt.strftime("%b-%Y"),
        )
    else:
        df = df.assign(ym=pd.NaT, year_month=pd.NA)
    return df


app.layout = html.Div(
    className="app",
    children=[
        html.Div(
            className="header",
            children=[
                html.Div("SHA Disbursements Dashboard", className="title"),
                html.Div(
                    "Filter by facility/vendor,  month, year.",
                    className="subtitle",
                ),
            ],
        ),
        html.Div(
            id="no-data-banner",
            className="notice",
            children="",  # populated by callback when no data after filters
        ),
        html.Div(
            className="filters",
            children=[
                html.Div(
                    className="filter",
                    children=[
                        html.Label("Facility / Vendor"),
                        dcc.Dropdown(
                            id="vendor-dd",
                            options=[
                                {"label": v, "value": v} for v in FILTER_META["vendors"]
                            ],
                            multi=True,
                            placeholder="Select facilities",
                        ),
                    ],
                ),
                # html.Div(
                #     className="filter",
                #     children=[
                #         html.Label("County"),
                #         dcc.Dropdown(
                #             id="county-dd",
                #             options=(
                #                 [
                #                     {"label": c, "value": c}
                #                     for c in sorted(RAW["county"].dropna().unique())
                #                 ]
                #                 if "county" in RAW.columns
                #                 else []
                #             ),
                #             multi=True,
                #             placeholder="Select counties (optional)",
                #         ),
                #     ],
                # ),
                # html.Div(
                #     className="filter",
                #     children=[
                #         html.Label("Sub-County"),
                #         dcc.Dropdown(
                #             id="subcounty-dd",
                #             options=(
                #                 [
                #                     {"label": s, "value": s}
                #                     for s in sorted(RAW["sub_county"].dropna().unique())
                #                 ]
                #                 if "sub_county" in RAW.columns
                #                 else []
                #             ),
                #             multi=True,
                #             placeholder="Select sub-counties (optional)",
                #         ),
                #     ],
                # ),
                html.Div(
                    className="filter",
                    children=[
                        html.Label("Report Month"),
                        dcc.Dropdown(
                            id="month-dd",
                            options=[
                                {"label": m, "value": m} for m in FILTER_META["months"]
                            ],
                            multi=True,
                            placeholder="Select months",
                        ),
                    ],
                ),
                html.Div(
                    className="filter",
                    children=[
                        html.Label("Report Year"),
                        dcc.Dropdown(
                            id="year-dd",
                            options=[
                                {"label": int(y), "value": int(y)}
                                for y in FILTER_META["years"]
                            ],
                            multi=True,
                            placeholder="Select years",
                        ),
                    ],
                ),
                html.Button("Apply", id="apply-btn", className="apply-btn", n_clicks=0),
            ],
        ),
        html.Div(
            className="cards",
            children=[
                stat_card("Total Disbursed (KES)", 0, "total-amount"),
                stat_card("Total Facilities", 0, "total-facilities"),
                stat_card("Rows", 0, "total-rows"),
            ],
        ),
        html.Div(
            className="content",
            children=[
                html.Div(
                    className="panel chart-panel",
                    children=[
                        html.Div("Top Facilities by Amount", className="panel-title"),
                        dcc.Graph(
                            id="top-vendors-chart",
                            style={"height": "420px", "width": "100%"},
                            config={"responsive": True},
                            animate=False,
                            clear_on_unhover=True,
                        ),
                    ],
                ),
                html.Div(
                    className="panel chart-panel",
                    children=[
                        html.Div("Disbursement by Month", className="panel-title"),
                        dcc.Graph(
                            id="by-month-chart",
                            style={"height": "420px", "width": "100%"},
                            config={"responsive": True},
                            animate=False,
                            clear_on_unhover=True,
                        ),
                    ],
                ),
                html.Div(
                    className="panel chart-panel",
                    children=[
                        html.Div("Hierarchy (Sunburst)", className="panel-title"),
                        dcc.Graph(
                            id="sunburst-chart",
                            style={"height": "420px", "width": "100%"},
                            config={"responsive": True},
                            animate=False,
                            clear_on_unhover=True,
                        ),
                    ],
                ),
                html.Div(
                    className="panel",
                    children=[
                        html.Div("Filtered Rows (first 500)", className="panel-title"),
                        dash_table.DataTable(
                            id="table",
                            columns=[
                                {"name": "vendor_name", "id": "vendor_name"},
                                amount_col,
                                {"name": "report_month", "id": "report_month"},
                                {"name": "report_year", "id": "report_year"},
                                {"name": "schedule", "id": "schedule"},
                                *(
                                    [{"name": "county", "id": "county"}]
                                    if "county" in RAW.columns
                                    else []
                                ),
                                *(
                                    [{"name": "sub_county", "id": "sub_county"}]
                                    if "sub_county" in RAW.columns
                                    else []
                                ),
                            ],
                            data=[],
                            page_size=10,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto"},
                            style_cell={
                                "backgroundColor": "var(--panel-bg)",
                                "color": "var(--text)",
                                "border": "1px solid var(--muted)",
                                "fontSize": "10px",
                            },
                            style_header={
                                "backgroundColor": "var(--panel-darker)",
                                "fontWeight": "bold",
                            },
                        ),
                    ],
                ),
            ],
        ),
        dcc.Store(id="filtered-json", data=INITIAL_JSON),  # seeded once
        html.Div(
            className="footer",
            children="Built for quick monitoring of SHA disbursements.",
        ),
    ],
)


# Apply filters only when button is clicked (no initial fire)
@app.callback(
    Output("filtered-json", "data"),
    Input("apply-btn", "n_clicks"),
    State("vendor-dd", "value"),
    # State("county-dd", "value"),
    # State("subcounty-dd", "value"),
    State("month-dd", "value"),
    State("year-dd", "value"),
    prevent_initial_call=True,
)
def apply_filters(n_clicks, vendors, months, years):
    df = fx.filter_data(RAW, vendors, months, years)  # existing filters
    df = df.where(pd.notna(df), None)  # JSON-stable
    return df.to_json(date_format="iso", orient="split")


@app.callback(
    Output("no-data-banner", "children"),
    Output("top-vendors-chart", "figure"),
    Output("by-month-chart", "figure"),
    Output("sunburst-chart", "figure"),
    Output("total-amount", "children"),
    Output("total-facilities", "children"),
    Output("total-rows", "children"),
    Output("table", "data"),
    Input("filtered-json", "data"),
)
def update_views(json_df):
    if not json_df:
        df = RAW.copy()
    else:
        df = pd.read_json(StringIO(json_df), orient="split")

    # Banner + empty figures helper
    def empty_fig():
        f = px.bar(pd.DataFrame({"x": [], "y": []}), x="x", y="y")
        f.update_layout(
            autosize=True,
            height=420,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="var(--panel-bg)",
            plot_bgcolor="var(--panel-bg)",
            font_color="var(--text)",
        )
        return f

    if df.empty:
        banner = "No data available at the moment"
        return (
            banner,
            empty_fig(),  # top vendors
            empty_fig(),  # by month
            empty_fig(),  # sunburst
            "0.00",  # total amount
            "0",  # total facilities
            "0",  # rows
            [],  # table
        )

    # Totals
    t = fx.totals(df)
    tot_amount = f"{t['total_amount']:,.2f}"
    tot_f = f"{t['total_facilities']:,}"
    rows = f"{t['rows']:,}"

    # 1) Top vendors bar
    tv = fx.top_vendors(df, k=20)
    if tv.empty:
        fig_bar = empty_fig()
    else:
        fig_bar = px.bar(tv, x="amount", y="vendor_name", orientation="h")
        fig_bar.update_layout(
            autosize=True,
            height=420,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="var(--panel-bg)",
            plot_bgcolor="var(--panel-bg)",
            font_color="var(--text)",
            xaxis_title="KES",
            yaxis_title="",
        )

    # 2) Disbursement by month — labels 'Apr-2025' with chronological ordering
    dfm = make_month_key(df)
    if "ym" in dfm.columns and dfm["ym"].notna().any():
        bym = (
            dfm.dropna(subset=["ym"])
            .groupby("ym", as_index=False)["amount"]
            .sum()
            .sort_values("ym")
        )
        bym["year_month"] = bym["ym"].dt.strftime("%b-%Y")
        fig_month = px.bar(bym, x="year_month", y="amount")
        fig_month.update_xaxes(
            categoryorder="array", categoryarray=bym["year_month"].tolist()
        )
        fig_month.update_layout(
            autosize=True,
            height=420,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="var(--panel-bg)",
            plot_bgcolor="var(--panel-bg)",
            font_color="var(--text)",
            xaxis_title="Month-Year",
            yaxis_title="Amount in KES",
        )
    else:
        fig_month = empty_fig()

    # 4) Sunburst (county → sub_county) else (year → month)
    if {"county", "sub_county"}.issubset(df.columns) and df[
        ["county", "sub_county"]
    ].notna().any().any():
        sun = df.groupby(["county", "sub_county"], as_index=False)["amount"].sum()
        if sun.empty:
            fig_sun = empty_fig()
        else:
            fig_sun = px.sunburst(sun, path=["county", "sub_county"], values="amount")
    elif {"report_year", "report_month"}.issubset(df.columns):
        sun = dfm.groupby(["report_year", "report_month"], as_index=False)[
            "amount"
        ].sum()
        if sun.empty:
            fig_sun = empty_fig()
        else:
            fig_sun = px.sunburst(
                sun, path=["report_year", "report_month"], values="amount"
            )
    else:
        fig_sun = empty_fig()

    for fig in (fig_sun,):
        fig.update_layout(
            autosize=True,
            height=420,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="var(--panel-bg)",
            plot_bgcolor="var(--panel-bg)",
            font_color="var(--text)",
        )

    preview = df.head(500).to_dict("records")
    return (
        "",  # no banner
        fig_bar,
        fig_month,
        fig_sun,
        tot_amount,
        tot_f,
        rows,
        preview,
    )


if __name__ == "__main__":
    app.run(debug=False, use_reloader=False, host="0.0.0.0", port=8050)
