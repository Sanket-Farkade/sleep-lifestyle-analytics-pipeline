# dashboard.py
"""
Sleep & Lifestyle Health Analytics — Plotly Dash Dashboard
16 charts, auto-refresh every 30 seconds.
Reads Parquet from local output/ folder (or S3 path via OUTPUT_BASE env-var).

Run:
    python dashboard.py
    Open: http://localhost:8050
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc

# ── CONFIG ────────────────────────────────────────────────────────────────────
_here = os.path.dirname(os.path.abspath(__file__))
BASE  = os.environ.get("OUTPUT_BASE", os.path.join(_here, "output"))
LIFESTYLE_PATH  = os.path.join(BASE, "lifestyle")
PERSONAL_PATH   = os.path.join(BASE, "personal")
PROFESSION_PATH = os.path.join(BASE, "profession")
REFRESH_MS      = 30_000   # 30 seconds

print(f"[dashboard] Reading Parquet from: {BASE}")

# ── COLOUR PALETTE ────────────────────────────────────────────────────────────
C = {
    "bg":      "#0a0e1a",
    "surface": "#111827",
    "card":    "#161d2e",
    "border":  "#1e2d45",
    "cyan":    "#00d4ff",
    "violet":  "#7c3aed",
    "amber":   "#f59e0b",
    "emerald": "#10b981",
    "rose":    "#ef4444",
    "text":    "#e2e8f0",
    "muted":   "#64748b",
    "grid":    "#1a2540",
}
CHART_PALETTE = [C["cyan"], C["violet"], C["amber"], C["emerald"], C["rose"],
                 "#a78bfa", "#34d399", "#fbbf24", "#60a5fa", "#f87171"]

# ── BASE LAYOUT APPLIED TO EVERY FIGURE ───────────────────────────────────────
BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Mono, monospace", color=C["text"], size=11),
    margin=dict(l=10, r=10, t=30, b=10),
    xaxis=dict(gridcolor=C["grid"], zerolinecolor=C["grid"], tickfont=dict(size=10)),
    yaxis=dict(gridcolor=C["grid"], zerolinecolor=C["grid"], tickfont=dict(size=10)),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
    colorway=CHART_PALETTE,
)


# ── DATA LOADING ──────────────────────────────────────────────────────────────

# Cache last known good DataFrames — if Spark is mid-write we return
# the previous batch instead of showing "Waiting for data"
_cache = {"lifestyle": pd.DataFrame(), "personal": pd.DataFrame(), "profession": pd.DataFrame()}


def load_parquet(path: str, cache_key: str) -> pd.DataFrame:
    """
    Recursively find all .parquet files under *path* (handles Spark's
    dated subdirectories). Falls back to cached data if mid-write or empty.
    """
    try:
        if not os.path.exists(path):
            return _cache[cache_key]

        # Walk all subdirectories to find parquet files
        files = []
        for root, dirs, filenames in os.walk(path):
            # Skip Spark's temporary write folders
            dirs[:] = [d for d in dirs if not d.startswith("_")]
            for f in filenames:
                # Match both .parquet and .snappy.parquet, skip CRC and temp files
                if (".parquet" in f) and not f.startswith("_") and not f.endswith(".crc"):
                    files.append(os.path.join(root, f))

        if not files:
            return _cache[cache_key]

        # Skip zero-byte files — Spark writes empty placeholder files
        # during micro-batch commits which cause pyarrow to crash
        files = [f for f in files if os.path.getsize(f) > 0]

        if not files:
            return _cache[cache_key]

        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception as e:
                print(f"[WARN] Skipping bad file {f}: {e}")
                continue

        if not dfs:
            return _cache[cache_key]

        df = pd.concat(dfs, ignore_index=True)

        if not df.empty:
            _cache[cache_key] = df  # update cache only on successful read

        return df

    except Exception as exc:
        print(f"[WARN] load_parquet({path}): {exc}")
        return _cache[cache_key]  # return last good data on any error


def load_all():
    return (
        load_parquet(LIFESTYLE_PATH,  "lifestyle"),
        load_parquet(PERSONAL_PATH,   "personal"),
        load_parquet(PROFESSION_PATH, "profession"),
    )


# ── UI HELPERS ────────────────────────────────────────────────────────────────
def empty_fig(msg="Waiting for data …"):
    fig = go.Figure()
    fig.add_annotation(text=msg, x=0.5, y=0.5, xref="paper", yref="paper",
                       showarrow=False, font=dict(color=C["muted"], size=13))
    fig.update_layout(**BASE_LAYOUT)
    return fig


def card(title: str, children, span: int = 4):
    return html.Div([
        html.Div(title, style={
            "fontSize": "10px", "letterSpacing": "2px", "textTransform": "uppercase",
            "color": C["muted"], "marginBottom": "12px", "fontFamily": "DM Mono, monospace",
        }),
        *children,
    ], style={
        "background": C["card"],
        "border": f"1px solid {C['border']}",
        "borderRadius": "12px",
        "padding": "20px",
        "gridColumn": f"span {span}",
    })


def kpi(label, value, unit="", color=C["cyan"]):
    return html.Div([
        html.Div(label, style={
            "fontSize": "9px", "letterSpacing": "2px", "textTransform": "uppercase",
            "color": C["muted"], "marginBottom": "8px", "fontFamily": "DM Mono, monospace",
        }),
        html.Div([
            html.Span(value, style={
                "fontSize": "30px", "fontWeight": "700", "color": color,
                "fontFamily": "DM Mono, monospace", "letterSpacing": "-1px",
            }),
            html.Span(f" {unit}", style={"fontSize": "12px", "color": C["muted"]}),
        ]),
    ], style={
        "background": C["card"],
        "border": f"1px solid {C['border']}",
        "borderRadius": "12px",
        "padding": "18px 22px",
        "borderLeft": f"3px solid {color}",
    })


def grid(cols, gap="16px"):
    return {"display": "grid", "gridTemplateColumns": cols,
            "gap": gap, "marginBottom": "20px"}


# ── APP ───────────────────────────────────────────────────────────────────────
app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500"
        "&family=Space+Grotesk:wght@400;600;700&display=swap",
    ],
    title="Sleep Health Analytics",
)

app.layout = html.Div([
    dcc.Interval(id="tick", interval=REFRESH_MS, n_intervals=0),

    # ── HEADER ────────────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            html.Div("SLEEP // HEALTH", style={
                "fontSize": "22px", "fontWeight": "700", "color": C["cyan"],
                "fontFamily": "DM Mono, monospace", "letterSpacing": "3px",
            }),
            html.Div("Real-Time Lifestyle Analytics Pipeline", style={
                "fontSize": "12px", "color": C["muted"], "marginTop": "2px",
                "fontFamily": "DM Mono, monospace",
            }),
        ]),
        html.Div([
            html.Div(id="last-updated", style={
                "fontSize": "10px", "color": C["muted"],
                "fontFamily": "DM Mono, monospace", "textAlign": "right",
            }),
            html.Div([
                html.Span("● ", style={"color": C["emerald"]}),
                html.Span("LIVE", style={"color": C["emerald"], "fontSize": "10px",
                                         "letterSpacing": "2px",
                                         "fontFamily": "DM Mono, monospace"}),
            ], style={"marginTop": "4px", "textAlign": "right"}),
        ]),
    ], style={
        "display": "flex", "justifyContent": "space-between", "alignItems": "center",
        "padding": "24px 32px", "borderBottom": f"1px solid {C['border']}",
        "background": C["surface"],
    }),

    # ── BODY ──────────────────────────────────────────────────────────────────
    html.Div([

        # KPI row
        html.Div(id="kpi-row", style=grid("repeat(5, 1fr)")),

        # Row 2: Sleep hist | Quality gauge | Stress scatter
        html.Div([
            card("Sleep Duration Distribution",
                 [dcc.Graph(id="g-sleep-dist", config={"displayModeBar": False},
                            style={"height": "240px"})], span=5),
            card("Avg Sleep Quality",
                 [dcc.Graph(id="g-quality-gauge", config={"displayModeBar": False},
                            style={"height": "240px"})], span=3),
            card("Stress vs Sleep Quality",
                 [dcc.Graph(id="g-stress-scatter", config={"displayModeBar": False},
                            style={"height": "240px"})], span=4),
        ], style=grid("repeat(12, 1fr)")),

        # Row 3: Timeline | Wellbeing gauge | Sleep category donut
        html.Div([
            card("Sleep Duration Over Time",
                 [dcc.Graph(id="g-timeline", config={"displayModeBar": False},
                            style={"height": "220px"})], span=6),
            card("Wellbeing Index",
                 [dcc.Graph(id="g-wellbeing", config={"displayModeBar": False},
                            style={"height": "220px"})], span=3),
            card("Sleep Category Split",
                 [dcc.Graph(id="g-sleep-cat", config={"displayModeBar": False},
                            style={"height": "220px"})], span=3),
        ], style=grid("repeat(12, 1fr)")),

        # Row 4: BMI distribution | Activity level | Hydration status
        html.Div([
            card("BMI Category Distribution",
                 [dcc.Graph(id="g-bmi", config={"displayModeBar": False},
                            style={"height": "220px"})], span=4),
            card("Activity Level by Persona",
                 [dcc.Graph(id="g-activity", config={"displayModeBar": False},
                            style={"height": "220px"})], span=5),
            card("Hydration Status",
                 [dcc.Graph(id="g-hydration", config={"displayModeBar": False},
                            style={"height": "220px"})], span=3),
        ], style=grid("repeat(12, 1fr)")),

        # Row 5: Burnout by industry | Work hrs vs Sleep | Remote pie
        html.Div([
            card("Burnout Risk by Industry",
                 [dcc.Graph(id="g-burnout", config={"displayModeBar": False},
                            style={"height": "220px"})], span=5),
            card("Work Hours vs Sleep Duration",
                 [dcc.Graph(id="g-work-sleep", config={"displayModeBar": False},
                            style={"height": "220px"})], span=4),
            card("Remote vs Onsite",
                 [dcc.Graph(id="g-remote", config={"displayModeBar": False},
                            style={"height": "220px"})], span=3),
        ], style=grid("repeat(12, 1fr)")),

        # Row 6: Lifestyle correlations | Sleep disorder donut | Overwork by industry
        html.Div([
            card("Lifestyle Factors vs Sleep Quality",
                 [dcc.Graph(id="g-corr", config={"displayModeBar": False},
                            style={"height": "220px"})], span=5),
            card("Sleep Disorder Prevalence",
                 [dcc.Graph(id="g-disorder", config={"displayModeBar": False},
                            style={"height": "220px"})], span=3),
            card("Overwork % by Industry",
                 [dcc.Graph(id="g-overwork", config={"displayModeBar": False},
                            style={"height": "220px"})], span=4),
        ], style=grid("repeat(12, 1fr)")),

        # Row 7: Gender pie | Age group bar
        html.Div([
            card("Gender Distribution",
                 [dcc.Graph(id="g-gender", config={"displayModeBar": False},
                            style={"height": "220px"})], span=4),
            card("Age Group Breakdown",
                 [dcc.Graph(id="g-age", config={"displayModeBar": False},
                            style={"height": "220px"})], span=4),
            card("Work Stress by Industry",
                 [dcc.Graph(id="g-industry-stress", config={"displayModeBar": False},
                            style={"height": "220px"})], span=4),
        ], style=grid("repeat(12, 1fr)")),

    ], style={"padding": "24px 32px", "background": C["bg"], "minHeight": "100vh"}),

], style={"background": C["bg"], "minHeight": "100vh"})


# ── CALLBACK ──────────────────────────────────────────────────────────────────
@app.callback(
    Output("kpi-row",           "children"),
    Output("last-updated",      "children"),
    Output("g-sleep-dist",      "figure"),
    Output("g-quality-gauge",   "figure"),
    Output("g-stress-scatter",  "figure"),
    Output("g-timeline",        "figure"),
    Output("g-wellbeing",       "figure"),
    Output("g-sleep-cat",       "figure"),
    Output("g-bmi",             "figure"),
    Output("g-activity",        "figure"),
    Output("g-hydration",       "figure"),
    Output("g-burnout",         "figure"),
    Output("g-work-sleep",      "figure"),
    Output("g-remote",          "figure"),
    Output("g-corr",            "figure"),
    Output("g-disorder",        "figure"),
    Output("g-overwork",        "figure"),
    Output("g-gender",          "figure"),
    Output("g-age",             "figure"),
    Output("g-industry-stress", "figure"),
    Input("tick", "n_intervals"),
)
def update_all(_n):
    lf, pf, pr = load_all()
    now = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")

    has_lf = not lf.empty
    has_pf = not pf.empty
    has_pr = not pr.empty

    # ── KPIs ──────────────────────────────────────────────────────────────────
    total   = len(lf) if has_lf else 0
    avg_sl  = f"{lf['sleep_duration_hrs'].mean():.1f}" if has_lf else "—"
    avg_q   = f"{lf['sleep_quality'].mean():.1f}"      if has_lf else "—"
    avg_st  = f"{lf['stress_level'].mean():.1f}"       if has_lf else "—"
    avg_wb  = f"{lf['wellbeing_index'].mean():.1f}"    if (has_lf and "wellbeing_index" in lf) else "—"

    kpis = html.Div([
        kpi("Total Records",   f"{total:,}",  "records", C["cyan"]),
        kpi("Avg Sleep",       avg_sl,         "hrs",     C["violet"]),
        kpi("Avg Quality",     avg_q,          "/ 10",    C["emerald"]),
        kpi("Avg Stress",      avg_st,         "/ 10",    C["amber"]),
        kpi("Avg Wellbeing",   avg_wb,         "/ 10",    C["rose"]),
    ], style={"display": "contents"})

    # ── 1. Sleep duration histogram ───────────────────────────────────────────
    if has_lf:
        fig_dist = go.Figure(go.Histogram(
            x=lf["sleep_duration_hrs"], nbinsx=22,
            marker=dict(color=C["cyan"], opacity=0.85,
                        line=dict(color=C["bg"], width=0.5)),
        ))
        fig_dist.update_layout(**BASE_LAYOUT, bargap=0.04)
        fig_dist.update_xaxes(title_text="Hours")
        fig_dist.update_yaxes(title_text="Count")
    else:
        fig_dist = empty_fig()

    # ── 2. Sleep quality gauge ────────────────────────────────────────────────
    avg_q_val = lf["sleep_quality"].mean() if has_lf else 0
    if has_lf:
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=round(avg_q_val, 1),
            domain={"x": [0, 1], "y": [0, 1]},
            gauge={
                "axis": {"range": [0, 10], "tickcolor": C["muted"]},
                "bar":  {"color": C["emerald"]},
                "bgcolor": C["card"],
                "bordercolor": C["border"],
                "steps": [
                    {"range": [0, 4],  "color": "#1a1f2e"},
                    {"range": [4, 7],  "color": "#1e2d3a"},
                    {"range": [7, 10], "color": "#1a2e25"},
                ],
                "threshold": {"line": {"color": C["cyan"], "width": 2},
                              "thickness": 0.75, "value": avg_q_val},
            },
            number={"font": {"color": C["emerald"], "size": 38, "family": "DM Mono"}},
        ))
        fig_gauge.update_layout(**BASE_LAYOUT)
    else:
        fig_gauge = empty_fig()

    # ── 3. Stress vs Sleep Quality scatter ────────────────────────────────────
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
        fig_scatter.update_layout(**BASE_LAYOUT)
        fig_scatter.update_xaxes(title_text="Stress Level")
        fig_scatter.update_yaxes(title_text="Sleep Quality")
    else:
        fig_scatter = empty_fig()

    # ── 4. Timeline ───────────────────────────────────────────────────────────
    if has_lf and "timestamp" in lf.columns:
        try:
            lf["_ts"] = pd.to_datetime(lf["timestamp"])
            tl = lf.sort_values("_ts").tail(300)
            fig_time = go.Figure()
            fig_time.add_trace(go.Scatter(
                x=tl["_ts"], y=tl["sleep_duration_hrs"],
                mode="lines", name="Sleep hrs",
                line=dict(color=C["cyan"], width=1.5),
                fill="tozeroy", fillcolor="rgba(0,212,255,0.07)",
            ))
            fig_time.add_trace(go.Scatter(
                x=tl["_ts"], y=tl["stress_level"],
                mode="lines", name="Stress",
                line=dict(color=C["rose"], width=1.2, dash="dot"),
                yaxis="y2",
            ))
            fig_time.update_layout(
                **BASE_LAYOUT,
                yaxis2=dict(overlaying="y", side="right",
                            gridcolor=C["grid"], tickfont=dict(size=10), range=[0, 10]),
            )
        except Exception:
            fig_time = empty_fig()
    else:
        fig_time = empty_fig()

    # ── 5. Wellbeing gauge ────────────────────────────────────────────────────
    wb_val = lf["wellbeing_index"].mean() if (has_lf and "wellbeing_index" in lf.columns) else 0
    if has_lf and "wellbeing_index" in lf.columns:
        fig_wb = go.Figure(go.Indicator(
            mode="gauge+number",
            value=round(wb_val, 1),
            domain={"x": [0, 1], "y": [0, 1]},
            gauge={
                "axis": {"range": [0, 10], "tickcolor": C["muted"]},
                "bar":  {"color": C["violet"]},
                "bgcolor": C["card"],
                "bordercolor": C["border"],
                "steps": [
                    {"range": [0, 4],  "color": "#1a1f2e"},
                    {"range": [4, 7],  "color": "#1e2a38"},
                    {"range": [7, 10], "color": "#1e1a2e"},
                ],
            },
            number={"font": {"color": C["violet"], "size": 38, "family": "DM Mono"}},
        ))
        fig_wb.update_layout(**BASE_LAYOUT)
    else:
        fig_wb = empty_fig()

    # ── 6. Sleep category donut ───────────────────────────────────────────────
    if has_lf and "sleep_category" in lf.columns:
        sc = lf["sleep_category"].value_counts()
        fig_scat = go.Figure(go.Pie(
            labels=sc.index, values=sc.values, hole=0.6,
            marker=dict(colors=[C["emerald"], C["cyan"], C["amber"], C["rose"]],
                        line=dict(color=C["bg"], width=2)),
            textfont=dict(size=10, family="DM Mono"),
        ))
        fig_scat.update_layout(**BASE_LAYOUT)
    else:
        fig_scat = empty_fig()

    # ── 7. BMI category bar ───────────────────────────────────────────────────
    if has_lf and "bmi_category" in lf.columns:
        order = ["underweight", "normal", "overweight", "obese"]
        bmi_counts = lf["bmi_category"].value_counts().reindex(order, fill_value=0)
        fig_bmi = go.Figure(go.Bar(
            x=bmi_counts.index, y=bmi_counts.values,
            marker=dict(color=[C["cyan"], C["emerald"], C["amber"], C["rose"]],
                        opacity=0.85, line=dict(color=C["bg"], width=0.5)),
            text=bmi_counts.values, textposition="outside",
            textfont=dict(size=9, color=C["muted"]),
        ))
        fig_bmi.update_layout(**BASE_LAYOUT)
    else:
        fig_bmi = empty_fig()

    # ── 8. Activity level by persona (stacked bar) ────────────────────────────
    if has_lf and "activity_level" in lf.columns and "persona" in lf.columns:
        act = (lf.groupby(["persona", "activity_level"])
                 .size().reset_index(name="count"))
        levels = ["very_active", "active", "lightly_active", "sedentary"]
        level_colors = [C["emerald"], C["cyan"], C["amber"], C["rose"]]
        fig_act = go.Figure()
        for lvl, color in zip(levels, level_colors):
            d = act[act["activity_level"] == lvl]
            fig_act.add_trace(go.Bar(
                name=lvl, x=d["persona"], y=d["count"],
                marker_color=color,
            ))
        fig_act.update_layout(**BASE_LAYOUT, barmode="stack")
        fig_act.update_xaxes(tickangle=-25, tickfont=dict(size=8))
    else:
        fig_act = empty_fig()

    # ── 9. Hydration status donut ─────────────────────────────────────────────
    if has_lf and "hydration_status" in lf.columns:
        hy = lf["hydration_status"].value_counts()
        fig_hyd = go.Figure(go.Pie(
            labels=hy.index, values=hy.values, hole=0.55,
            marker=dict(colors=[C["rose"], C["cyan"], C["emerald"]],
                        line=dict(color=C["bg"], width=2)),
            textfont=dict(size=10, family="DM Mono"),
        ))
        fig_hyd.update_layout(**BASE_LAYOUT)
    else:
        fig_hyd = empty_fig()

    # ── 10. Burnout risk by industry ──────────────────────────────────────────
    if has_pr and "burnout_risk_index" in pr.columns:
        burn = (pr.groupby("industry")["burnout_risk_index"]
                  .mean().sort_values(ascending=True))
        colors_b = [C["rose"] if v >= 70 else C["amber"] if v >= 50 else C["emerald"]
                    for v in burn.values]
        fig_burn = go.Figure(go.Bar(
            y=burn.index, x=burn.values, orientation="h",
            marker=dict(color=colors_b, line=dict(color=C["bg"], width=0.5)),
            text=[f"{v:.1f}" for v in burn.values],
            textposition="outside",
            textfont=dict(size=9, color=C["muted"]),
        ))
        fig_burn.update_layout(**BASE_LAYOUT)
        fig_burn.update_xaxes(range=[0, 105])
    else:
        fig_burn = empty_fig()

    # ── 11. Work hours vs Sleep scatter ───────────────────────────────────────
    if has_lf and has_pr:
        try:
            merged = pd.merge(
                lf[["user_id", "sleep_duration_hrs", "stress_level"]],
                pr[["user_id", "work_hours_per_day", "industry"]],
                on="user_id", how="inner",
            )
            if not merged.empty:
                fig_ws = go.Figure(go.Scatter(
                    x=merged["work_hours_per_day"],
                    y=merged["sleep_duration_hrs"],
                    mode="markers",
                    marker=dict(
                        color=merged["stress_level"], colorscale="RdYlGn_r",
                        size=6, opacity=0.65,
                        colorbar=dict(title="Stress", thickness=10,
                                      tickfont=dict(color=C["muted"], size=9)),
                        line=dict(width=0),
                    ),
                ))
                fig_ws.update_layout(**BASE_LAYOUT)
                fig_ws.update_xaxes(title_text="Work hrs/day")
                fig_ws.update_yaxes(title_text="Sleep hrs")
            else:
                fig_ws = empty_fig("Waiting for joined data …")
        except Exception:
            fig_ws = empty_fig()
    else:
        fig_ws = empty_fig()

    # ── 12. Remote vs Onsite pie ──────────────────────────────────────────────
    if has_pr and "remote_onsite" in pr.columns:
        rem = pr["remote_onsite"].value_counts()
        fig_rem = go.Figure(go.Pie(
            labels=rem.index, values=rem.values, hole=0.5,
            marker=dict(colors=[C["cyan"], C["violet"], C["amber"]],
                        line=dict(color=C["bg"], width=2)),
            textfont=dict(size=10, family="DM Mono"),
        ))
        fig_rem.update_layout(**BASE_LAYOUT)
    else:
        fig_rem = empty_fig()

    # ── 13. Lifestyle factors correlation bar ─────────────────────────────────
    if has_lf:
        factors = [c for c in
                   ["steps", "water_intake_L", "exercise_mins",
                    "caffeine_mg", "alcohol_units", "screen_time_before_bed_mins"]
                   if c in lf.columns]
        if factors and "sleep_quality" in lf.columns:
            corrs = lf[factors + ["sleep_quality"]].corr()["sleep_quality"].drop("sleep_quality")
            fig_corr = go.Figure(go.Bar(
                x=corrs.index, y=corrs.values,
                marker=dict(
                    color=[C["emerald"] if v > 0 else C["rose"] for v in corrs.values],
                    opacity=0.85, line=dict(color=C["bg"], width=0.5),
                ),
                text=[f"{v:.2f}" for v in corrs.values],
                textposition="outside",
                textfont=dict(size=9, color=C["muted"]),
            ))
            fig_corr.update_layout(**BASE_LAYOUT)
            fig_corr.update_yaxes(range=[-1, 1], zeroline=True,
                                   zerolinecolor=C["cyan"], zerolinewidth=1)
        else:
            fig_corr = empty_fig()
    else:
        fig_corr = empty_fig()

    # ── 14. Sleep disorder prevalence donut ───────────────────────────────────
    if has_pf and "sleep_disorder" in pf.columns:
        sd = pf["sleep_disorder"].value_counts()
        fig_dis = go.Figure(go.Pie(
            labels=sd.index, values=sd.values, hole=0.55,
            marker=dict(colors=[C["emerald"], C["rose"], C["violet"], C["amber"]],
                        line=dict(color=C["bg"], width=2)),
            textfont=dict(size=10, family="DM Mono"),
        ))
        fig_dis.update_layout(**BASE_LAYOUT)
    else:
        fig_dis = empty_fig()

    # ── 15. Overwork % by industry ────────────────────────────────────────────
    if has_pr and "overwork_flag" in pr.columns:
        ow = (pr.groupby("industry")["overwork_flag"]
                .mean()
                .mul(100)
                .sort_values(ascending=False))
        fig_ow = go.Figure(go.Bar(
            x=ow.index, y=ow.values,
            marker=dict(
                color=[C["rose"] if v > 60 else C["amber"] if v > 30 else C["emerald"]
                       for v in ow.values],
                opacity=0.85, line=dict(color=C["bg"], width=0.5),
            ),
            text=[f"{v:.0f}%" for v in ow.values],
            textposition="outside",
            textfont=dict(size=9, color=C["muted"]),
        ))
        fig_ow.update_layout(**BASE_LAYOUT)
        fig_ow.update_yaxes(title_text="%", range=[0, 110])
        fig_ow.update_xaxes(tickangle=-25)
    else:
        fig_ow = empty_fig()

    # ── 16. Gender pie ────────────────────────────────────────────────────────
    if has_pf and "gender" in pf.columns:
        gc = pf["gender"].value_counts()
        fig_gen = go.Figure(go.Pie(
            labels=gc.index, values=gc.values, hole=0.5,
            marker=dict(colors=[C["cyan"], C["violet"], C["amber"]],
                        line=dict(color=C["bg"], width=2)),
            textfont=dict(size=10, family="DM Mono"),
        ))
        fig_gen.update_layout(**BASE_LAYOUT)
    else:
        fig_gen = empty_fig()

    # ── 17. Age group bar ─────────────────────────────────────────────────────
    if has_pf and "age_group" in pf.columns:
        order = ["18-24", "25-34", "35-49", "50-64", "65+"]
        ag = pf["age_group"].value_counts().reindex(order, fill_value=0)
        fig_age = go.Figure(go.Bar(
            x=ag.index, y=ag.values,
            marker=dict(color=C["violet"], opacity=0.85,
                        line=dict(color=C["bg"], width=0.5)),
            text=ag.values, textposition="outside",
            textfont=dict(size=9, color=C["muted"]),
        ))
        fig_age.update_layout(**BASE_LAYOUT)
    else:
        fig_age = empty_fig()

    # ── 18. Work stress by industry bar ──────────────────────────────────────
    if has_pr and "work_stress_score" in pr.columns:
        ind = (pr.groupby("industry")["work_stress_score"]
                 .mean().sort_values(ascending=True))
        fig_ind = go.Figure(go.Bar(
            y=ind.index, x=ind.values, orientation="h",
            marker=dict(
                color=[C["rose"] if v > 7 else C["amber"] if v > 5 else C["emerald"]
                       for v in ind.values],
                line=dict(color=C["bg"], width=0.5),
            ),
            text=[f"{v:.1f}" for v in ind.values],
            textposition="outside",
            textfont=dict(size=9, color=C["muted"]),
        ))
        fig_ind.update_layout(**BASE_LAYOUT)
        fig_ind.update_xaxes(range=[0, 11])
    else:
        fig_ind = empty_fig()

    return (
        kpis, f"Last updated: {now}",
        fig_dist, fig_gauge, fig_scatter,
        fig_time, fig_wb, fig_scat,
        fig_bmi, fig_act, fig_hyd,
        fig_burn, fig_ws, fig_rem,
        fig_corr, fig_dis, fig_ow,
        fig_gen, fig_age, fig_ind,
    )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8050) folder empty folder