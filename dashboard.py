"""
Sleep & Lifestyle Health Analytics Dashboard
Run: python dashboard.py
Then open: http://localhost:8050
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc

# ── CONFIG ────────────────────────────────────────────────────────
BASE = r"D:\CDAC Projects\sleep-pipeline\output"
LIFESTYLE_PATH  = os.path.join(BASE, "lifestyle")
PERSONAL_PATH   = os.path.join(BASE, "personal")
PROFESSION_PATH = os.path.join(BASE, "profession")

REFRESH_INTERVAL = 30_000  # 30 seconds in ms

# ── COLORS ────────────────────────────────────────────────────────
C = {
    "bg":       "#0a0e1a",
    "surface":  "#111827",
    "card":     "#161d2e",
    "border":   "#1e2d45",
    "accent1":  "#00d4ff",   # cyan
    "accent2":  "#7c3aed",   # violet
    "accent3":  "#f59e0b",   # amber
    "accent4":  "#10b981",   # emerald
    "danger":   "#ef4444",
    "text":     "#e2e8f0",
    "muted":    "#64748b",
    "grid":     "#1a2540",
}

# ── DATA LOADER ───────────────────────────────────────────────────
def load_parquet(path):
    try:
        if not os.path.exists(path):
            return pd.DataFrame()
        files = [f for f in os.listdir(path) if f.endswith(".parquet")]
        if not files:
            return pd.DataFrame()
        dfs = [pd.read_parquet(os.path.join(path, f)) for f in files]
        return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return pd.DataFrame()

def load_all():
    lf = load_parquet(LIFESTYLE_PATH)
    pf = load_parquet(PERSONAL_PATH)
    pr = load_parquet(PROFESSION_PATH)
    return lf, pf, pr

# ── CHART HELPERS ─────────────────────────────────────────────────
CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Mono, monospace", color=C["text"], size=11),
    margin=dict(l=10, r=10, t=30, b=10),
    xaxis=dict(gridcolor=C["grid"], zerolinecolor=C["grid"], tickfont=dict(size=10)),
    yaxis=dict(gridcolor=C["grid"], zerolinecolor=C["grid"], tickfont=dict(size=10)),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
)

def card(title, children, span=4):
    return html.Div([
        html.Div(title, style={
            "fontSize": "10px", "letterSpacing": "2px", "textTransform": "uppercase",
            "color": C["muted"], "marginBottom": "12px", "fontFamily": "DM Mono, monospace"
        }),
        *children
    ], style={
        "background": C["card"],
        "border": f"1px solid {C['border']}",
        "borderRadius": "12px",
        "padding": "20px",
        "gridColumn": f"span {span}",
    })

def kpi_card(label, value, unit="", color=C["accent1"], delta=None):
    return html.Div([
        html.Div(label, style={
            "fontSize": "9px", "letterSpacing": "2px", "textTransform": "uppercase",
            "color": C["muted"], "marginBottom": "8px", "fontFamily": "DM Mono, monospace"
        }),
        html.Div([
            html.Span(value, style={
                "fontSize": "32px", "fontWeight": "700", "color": color,
                "fontFamily": "DM Mono, monospace", "letterSpacing": "-1px"
            }),
            html.Span(f" {unit}", style={"fontSize": "13px", "color": C["muted"]}),
        ]),
        html.Div(delta or "", style={"fontSize": "10px", "color": C["accent4"], "marginTop": "4px"}),
    ], style={
        "background": C["card"],
        "border": f"1px solid {C['border']}",
        "borderRadius": "12px",
        "padding": "20px 24px",
        "borderLeft": f"3px solid {color}",
    })

# ── APP INIT ──────────────────────────────────────────────────────
app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Space+Grotesk:wght@400;600;700&display=swap"
    ],
    title="Sleep Health Analytics"
)

# ── LAYOUT ────────────────────────────────────────────────────────
app.layout = html.Div([
    dcc.Interval(id="refresh", interval=REFRESH_INTERVAL, n_intervals=0),

    # ── HEADER
    html.Div([
        html.Div([
            html.Div("SLEEP//HEALTH", style={
                "fontSize": "22px", "fontWeight": "700", "color": C["accent1"],
                "fontFamily": "DM Mono, monospace", "letterSpacing": "3px"
            }),
            html.Div("Real-Time Lifestyle Analytics Pipeline", style={
                "fontSize": "12px", "color": C["muted"], "marginTop": "2px",
                "fontFamily": "DM Mono, monospace"
            }),
        ]),
        html.Div([
            html.Div(id="last-updated", style={
                "fontSize": "10px", "color": C["muted"], "fontFamily": "DM Mono, monospace",
                "textAlign": "right"
            }),
            html.Div([
                html.Span("● ", style={"color": C["accent4"]}),
                html.Span("LIVE", style={"color": C["accent4"], "fontSize": "10px",
                                          "letterSpacing": "2px", "fontFamily": "DM Mono, monospace"})
            ], style={"marginTop": "4px", "textAlign": "right"}),
        ])
    ], style={
        "display": "flex", "justifyContent": "space-between", "alignItems": "center",
        "padding": "24px 32px", "borderBottom": f"1px solid {C['border']}",
        "background": C["surface"],
    }),

    # ── BODY
    html.Div([

        # ── KPI ROW
        html.Div(id="kpi-row", style={
            "display": "grid",
            "gridTemplateColumns": "repeat(5, 1fr)",
            "gap": "16px",
            "marginBottom": "20px",
        }),

        # ── ROW 2: Sleep dist + Quality gauge + Stress heatmap
        html.Div([
            card("Sleep Duration Distribution", [
                dcc.Graph(id="sleep-dist", config={"displayModeBar": False},
                          style={"height": "240px"})
            ], span=5),
            card("Avg Sleep Quality", [
                dcc.Graph(id="quality-gauge", config={"displayModeBar": False},
                          style={"height": "240px"})
            ], span=3),
            card("Stress vs Sleep Quality", [
                dcc.Graph(id="stress-scatter", config={"displayModeBar": False},
                          style={"height": "240px"})
            ], span=4),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(12, 1fr)",
                  "gap": "16px", "marginBottom": "20px"}),

        # ── ROW 3: Timeline + Gender pie + Age groups
        html.Div([
            card("Sleep Duration Over Time", [
                dcc.Graph(id="timeline", config={"displayModeBar": False},
                          style={"height": "220px"})
            ], span=6),
            card("Gender Distribution", [
                dcc.Graph(id="gender-pie", config={"displayModeBar": False},
                          style={"height": "220px"})
            ], span=3),
            card("Age Group Breakdown", [
                dcc.Graph(id="age-bar", config={"displayModeBar": False},
                          style={"height": "220px"})
            ], span=3),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(12, 1fr)",
                  "gap": "16px", "marginBottom": "20px"}),

        # ── ROW 4: Industry stress + Work hours + Sleep category
        html.Div([
            card("Work Stress by Industry", [
                dcc.Graph(id="industry-stress", config={"displayModeBar": False},
                          style={"height": "220px"})
            ], span=5),
            card("Work Hours vs Sleep Duration", [
                dcc.Graph(id="work-sleep", config={"displayModeBar": False},
                          style={"height": "220px"})
            ], span=4),
            card("Sleep Category Split", [
                dcc.Graph(id="sleep-cat", config={"displayModeBar": False},
                          style={"height": "220px"})
            ], span=3),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(12, 1fr)",
                  "gap": "16px", "marginBottom": "20px"}),

        # ── ROW 5: Lifestyle factors correlation
        html.Div([
            card("Lifestyle Factors vs Sleep Quality", [
                dcc.Graph(id="lifestyle-corr", config={"displayModeBar": False},
                          style={"height": "220px"})
            ], span=8),
            card("Sleep Disorder Prevalence", [
                dcc.Graph(id="disorder-donut", config={"displayModeBar": False},
                          style={"height": "220px"})
            ], span=4),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(12, 1fr)",
                  "gap": "16px", "marginBottom": "20px"}),

    ], style={"padding": "24px 32px", "background": C["bg"], "minHeight": "100vh"}),

], style={"background": C["bg"], "minHeight": "100vh"})


# ── CALLBACKS ─────────────────────────────────────────────────────
@app.callback(
    Output("kpi-row", "children"),
    Output("last-updated", "children"),
    Output("sleep-dist", "figure"),
    Output("quality-gauge", "figure"),
    Output("stress-scatter", "figure"),
    Output("timeline", "figure"),
    Output("gender-pie", "figure"),
    Output("age-bar", "figure"),
    Output("industry-stress", "figure"),
    Output("work-sleep", "figure"),
    Output("sleep-cat", "figure"),
    Output("lifestyle-corr", "figure"),
    Output("disorder-donut", "figure"),
    Input("refresh", "n_intervals"),
)
def update_all(n):
    lf, pf, pr = load_all()
    now = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")

    # ── Fallback empty figures
    def empty_fig(msg="No data yet"):
        fig = go.Figure()
        fig.add_annotation(text=msg, x=0.5, y=0.5, showarrow=False,
                           font=dict(color=C["muted"], size=12))
        fig.update_layout(**CHART_LAYOUT)
        return fig

    has_lf = not lf.empty
    has_pf = not pf.empty
    has_pr = not pr.empty

    # ── KPIs
    total_records = len(lf) if has_lf else 0
    avg_sleep     = round(lf["sleep_duration_hrs"].mean(), 1) if has_lf else 0
    avg_quality   = round(lf["sleep_quality"].mean(), 1) if has_lf else 0
    avg_stress    = round(lf["stress_level"].mean(), 1) if has_lf else 0
    avg_steps     = int(lf["steps"].mean()) if has_lf else 0

    kpis = html.Div([
        kpi_card("Total Records", f"{total_records:,}", "records", C["accent1"]),
        kpi_card("Avg Sleep", str(avg_sleep), "hrs", C["accent2"]),
        kpi_card("Avg Quality", str(avg_quality), "/ 10", C["accent4"]),
        kpi_card("Avg Stress", str(avg_stress), "/ 10", C["accent3"]),
        kpi_card("Avg Daily Steps", f"{avg_steps:,}", "steps", C["danger"]),
    ], style={"display": "contents"})

    # ── Sleep duration histogram
    if has_lf:
        fig_dist = go.Figure(go.Histogram(
            x=lf["sleep_duration_hrs"], nbinsx=20,
            marker=dict(color=C["accent1"], opacity=0.85,
                        line=dict(color=C["bg"], width=0.5)),
        ))
        fig_dist.update_layout(title="", bargap=0.05, **CHART_LAYOUT)
        fig_dist.update_xaxes(title_text="Hours")
        fig_dist.update_yaxes(title_text="Count")
    else:
        fig_dist = empty_fig()

    # ── Quality gauge
    if has_lf:
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=avg_quality,
            domain={"x": [0, 1], "y": [0, 1]},
            gauge={
                "axis": {"range": [0, 10], "tickcolor": C["muted"]},
                "bar": {"color": C["accent4"]},
                "bgcolor": C["card"],
                "bordercolor": C["border"],
                "steps": [
                    {"range": [0, 4], "color": "#1a1f2e"},
                    {"range": [4, 7], "color": "#1e2d3a"},
                    {"range": [7, 10], "color": "#1a2e25"},
                ],
                "threshold": {"line": {"color": C["accent1"], "width": 2},
                              "thickness": 0.75, "value": avg_quality}
            },
            number={"font": {"color": C["accent4"], "size": 40, "family": "DM Mono"}},
        ))
        fig_gauge.update_layout(**CHART_LAYOUT)
    else:
        fig_gauge = empty_fig()

    # ── Stress vs Sleep Quality scatter
    if has_lf:
        fig_scatter = go.Figure(go.Scatter(
            x=lf["stress_level"], y=lf["sleep_quality"],
            mode="markers",
            marker=dict(
                color=lf["sleep_duration_hrs"], colorscale="Viridis",
                size=6, opacity=0.6,
                colorbar=dict(title="Sleep hrs", thickness=10,
                              tickfont=dict(color=C["muted"], size=9)),
                line=dict(width=0),
            ),
        ))
        fig_scatter.update_layout(**CHART_LAYOUT)
        fig_scatter.update_xaxes(title_text="Stress Level")
        fig_scatter.update_yaxes(title_text="Sleep Quality")
    else:
        fig_scatter = empty_fig()

    # ── Timeline
    if has_lf and "timestamp" in lf.columns:
        try:
            lf["ts"] = pd.to_datetime(lf["timestamp"])
            timeline_df = lf.sort_values("ts").tail(200)
            fig_time = go.Figure()
            fig_time.add_trace(go.Scatter(
                x=timeline_df["ts"], y=timeline_df["sleep_duration_hrs"],
                mode="lines", line=dict(color=C["accent1"], width=1.5),
                fill="tozeroy", fillcolor="rgba(0,212,255,0.07)", name="Sleep hrs"
            ))
            fig_time.add_trace(go.Scatter(
                x=timeline_df["ts"], y=timeline_df["sleep_quality"],
                mode="lines", line=dict(color=C["accent3"], width=1.5, dash="dot"),
                name="Quality", yaxis="y2"
            ))
            fig_time.update_layout(
                **CHART_LAYOUT,
                yaxis2=dict(overlaying="y", side="right", gridcolor=C["grid"],
                            tickfont=dict(size=10), range=[0, 10]),
            )
        except:
            fig_time = empty_fig()
    else:
        fig_time = empty_fig()

    # ── Gender pie
    if has_pf and "gender" in pf.columns:
        gc = pf["gender"].value_counts()
        fig_gender = go.Figure(go.Pie(
            labels=gc.index, values=gc.values,
            hole=0.5,
            marker=dict(colors=[C["accent1"], C["accent2"], C["accent3"]],
                        line=dict(color=C["bg"], width=2)),
            textfont=dict(size=10, family="DM Mono"),
        ))
        fig_gender.update_layout(**CHART_LAYOUT)
    else:
        fig_gender = empty_fig()

    # ── Age group bar
    if has_pf and "age_group" in pf.columns:
        ag = pf["age_group"].value_counts().sort_index()
        fig_age = go.Figure(go.Bar(
            x=ag.index, y=ag.values,
            marker=dict(color=C["accent2"], opacity=0.85,
                        line=dict(color=C["bg"], width=0.5)),
        ))
        fig_age.update_layout(**CHART_LAYOUT)
    else:
        fig_age = empty_fig()

    # ── Industry stress
    if has_pr and "industry" in pr.columns:
        ind = pr.groupby("industry")["work_stress_score"].mean().sort_values(ascending=True)
        colors = [C["danger"] if v > 7 else C["accent3"] if v > 5 else C["accent4"]
                  for v in ind.values]
        fig_ind = go.Figure(go.Bar(
            y=ind.index, x=ind.values, orientation="h",
            marker=dict(color=colors, line=dict(color=C["bg"], width=0.5)),
            text=[f"{v:.1f}" for v in ind.values],
            textposition="outside",
            textfont=dict(size=9, color=C["muted"]),
        ))
        fig_ind.update_layout(**CHART_LAYOUT)
        fig_ind.update_xaxes(range=[0, 11])
    else:
        fig_ind = empty_fig()

    # ── Work hours vs Sleep scatter
    if has_lf and has_pr:
        try:
            merged = pd.merge(
                lf[["user_id", "sleep_duration_hrs", "stress_level"]],
                pr[["user_id", "work_hours_per_day", "industry"]],
                on="user_id", how="inner"
            )
            if not merged.empty:
                fig_ws = go.Figure(go.Scatter(
                    x=merged["work_hours_per_day"],
                    y=merged["sleep_duration_hrs"],
                    mode="markers",
                    marker=dict(color=merged["stress_level"], colorscale="RdYlGn_r",
                                size=6, opacity=0.65,
                                colorbar=dict(title="Stress", thickness=10,
                                              tickfont=dict(color=C["muted"], size=9)),
                                line=dict(width=0)),
                ))
                fig_ws.update_layout(**CHART_LAYOUT)
                fig_ws.update_xaxes(title_text="Work hrs/day")
                fig_ws.update_yaxes(title_text="Sleep hrs")
            else:
                fig_ws = empty_fig("Waiting for joined data...")
        except:
            fig_ws = empty_fig()
    else:
        fig_ws = empty_fig()

    # ── Sleep category donut
    if has_lf and "sleep_category" in lf.columns:
        sc = lf["sleep_category"].value_counts()
        fig_scat = go.Figure(go.Pie(
            labels=sc.index, values=sc.values, hole=0.6,
            marker=dict(colors=[C["accent4"], C["accent1"], C["accent3"]],
                        line=dict(color=C["bg"], width=2)),
            textfont=dict(size=10, family="DM Mono"),
        ))
        fig_scat.update_layout(**CHART_LAYOUT)
    else:
        fig_scat = empty_fig()

    # ── Lifestyle correlations bar
    if has_lf:
        factors = ["steps", "water_intake_L", "exercise_mins",
                   "caffeine_mg", "alcohol_units", "screen_time_before_bed_mins"]
        factors = [f for f in factors if f in lf.columns]
        if factors:
            corrs = lf[factors + ["sleep_quality"]].corr()["sleep_quality"].drop("sleep_quality")
            colors_corr = [C["accent4"] if v > 0 else C["danger"] for v in corrs.values]
            fig_corr = go.Figure(go.Bar(
                x=corrs.index, y=corrs.values,
                marker=dict(color=colors_corr, opacity=0.85,
                            line=dict(color=C["bg"], width=0.5)),
                text=[f"{v:.2f}" for v in corrs.values],
                textposition="outside",
                textfont=dict(size=9, color=C["muted"]),
            ))
            fig_corr.update_layout(**CHART_LAYOUT, title="Correlation with Sleep Quality")
            fig_corr.update_yaxes(range=[-1, 1], zeroline=True,
                                   zerolinecolor=C["accent1"], zerolinewidth=1)
        else:
            fig_corr = empty_fig()
    else:
        fig_corr = empty_fig()

    # ── Sleep disorder donut
    if has_pf and "sleep_disorder" in pf.columns:
        sd = pf["sleep_disorder"].value_counts()
        fig_disorder = go.Figure(go.Pie(
            labels=sd.index, values=sd.values, hole=0.55,
            marker=dict(
                colors=[C["accent4"], C["danger"], C["accent2"], C["accent3"]],
                line=dict(color=C["bg"], width=2)
            ),
            textfont=dict(size=10, family="DM Mono"),
        ))
        fig_disorder.update_layout(**CHART_LAYOUT)
    else:
        fig_disorder = empty_fig()

    return (
        kpis, f"Last updated: {now}",
        fig_dist, fig_gauge, fig_scatter,
        fig_time, fig_gender, fig_age,
        fig_ind, fig_ws, fig_scat,
        fig_corr, fig_disorder,
    )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8050)