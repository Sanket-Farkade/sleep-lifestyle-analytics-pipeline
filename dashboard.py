# dashboard.py
"""
Workforce Wellbeing Analytics - HR / People Analytics Dashboard
Aggregate view across employee groups. Never shows individuals.

Focused chart set (12 total: 4 KPIs + 8 charts):
  KPIs:  Total Employees, Avg Wellbeing, Avg Burnout, % Overworked
  Row 1: Burnout Risk by Industry | Overwork % by Industry
  Row 2: Wellbeing Index by Industry | Work Stress by Industry
  Row 3: Sleep Category split | Stress Tier distribution
  Row 4: Gender Distribution | Age Group Breakdown

Reads Parquet from output/ (or OUTPUT_BASE env-var). Works without live Kafka.
"""

import os
import pandas as pd
from datetime import datetime

import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc

# -- CONFIG --
_here = os.path.dirname(os.path.abspath(__file__))
BASE  = os.environ.get("OUTPUT_BASE", os.path.join(_here, "output"))

LIFESTYLE_PATH  = os.path.join(BASE, "lifestyle")
PERSONAL_PATH   = os.path.join(BASE, "personal")
PROFESSION_PATH = os.path.join(BASE, "profession")
REFRESH_MS      = 30_000

print(f"[dashboard] Reading Parquet from: {BASE}")

# -- COLOUR PALETTE --
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
PALETTE = [C["cyan"], C["violet"], C["amber"], C["emerald"], C["rose"]]

BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Mono, monospace", color=C["text"], size=11),
    margin=dict(l=10, r=10, t=30, b=10),
    xaxis=dict(gridcolor=C["grid"], zerolinecolor=C["grid"], tickfont=dict(size=10)),
    yaxis=dict(gridcolor=C["grid"], zerolinecolor=C["grid"], tickfont=dict(size=10)),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
    colorway=PALETTE,
)

# -- DATA LOADING --
_cache = {"lifestyle": pd.DataFrame(), "personal": pd.DataFrame(), "profession": pd.DataFrame()}


def load_parquet(path, cache_key):
    try:
        if not os.path.exists(path):
            return _cache[cache_key]
        files = []
        for root, dirs, filenames in os.walk(path):
            dirs[:] = [d for d in dirs if not d.startswith("_")]
            for f in filenames:
                if (".parquet" in f) and not f.startswith("_") and not f.endswith(".crc"):
                    files.append(os.path.join(root, f))
        files = [f for f in files if os.path.getsize(f) > 0]
        if not files:
            return _cache[cache_key]
        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception:
                continue
        if not dfs:
            return _cache[cache_key]
        df = pd.concat(dfs, ignore_index=True)
        if not df.empty:
            _cache[cache_key] = df
        return df
    except Exception as exc:
        print(f"[WARN] load_parquet({path}): {exc}")
        return _cache[cache_key]


def load_all():
    return (
        load_parquet(LIFESTYLE_PATH,  "lifestyle"),
        load_parquet(PERSONAL_PATH,   "personal"),
        load_parquet(PROFESSION_PATH, "profession"),
    )


# -- UI HELPERS --
def empty_fig(msg="Waiting for data ..."):
    fig = go.Figure()
    fig.add_annotation(text=msg, x=0.5, y=0.5, xref="paper", yref="paper",
                       showarrow=False, font=dict(color=C["muted"], size=13))
    fig.update_layout(**BASE_LAYOUT)
    return fig


def card(title, children, span=6):
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


# -- APP --
app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500"
        "&family=Space+Grotesk:wght@400;600;700&display=swap",
    ],
    title="Workforce Wellbeing Analytics",
)

app.layout = html.Div([
    dcc.Interval(id="tick", interval=REFRESH_MS, n_intervals=0),

    # -- HEADER --
    html.Div([
        html.Div([
            html.Div("WORKFORCE WELLBEING", style={
                "fontSize": "22px", "fontWeight": "700", "color": C["cyan"],
                "fontFamily": "DM Mono, monospace", "letterSpacing": "3px",
            }),
            html.Div("People Analytics  |  Aggregate view across employee groups - no individual data",
                     style={
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
                html.Span("* ", style={"color": C["emerald"]}),
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

    html.Div([
        # KPI row
        html.Div(id="kpi-row", style=grid("repeat(4, 1fr)")),

        # Row 1: Burnout by industry | Overwork % by industry
        html.Div([
            card("Burnout Risk by Industry",
                 [dcc.Graph(id="g-burnout", config={"displayModeBar": False},
                            style={"height": "300px"})], span=6),
            card("Overwork Rate by Industry",
                 [dcc.Graph(id="g-overwork", config={"displayModeBar": False},
                            style={"height": "300px"})], span=6),
        ], style=grid("repeat(12, 1fr)")),

        # Row 2: Wellbeing by industry | Work stress by industry
        html.Div([
            card("Wellbeing Index by Industry",
                 [dcc.Graph(id="g-wellbeing", config={"displayModeBar": False},
                            style={"height": "300px"})], span=6),
            card("Work Stress by Industry",
                 [dcc.Graph(id="g-workstress", config={"displayModeBar": False},
                            style={"height": "300px"})], span=6),
        ], style=grid("repeat(12, 1fr)")),

        # Row 3: Sleep category | Stress tier
        html.Div([
            card("Sleep Adequacy Across Workforce",
                 [dcc.Graph(id="g-sleepcat", config={"displayModeBar": False},
                            style={"height": "280px"})], span=6),
            card("Stress Tier Distribution",
                 [dcc.Graph(id="g-stresstier", config={"displayModeBar": False},
                            style={"height": "280px"})], span=6),
        ], style=grid("repeat(12, 1fr)")),

        # Row 4: Gender | Age group  (diversity lens)
        html.Div([
            card("Gender Distribution",
                 [dcc.Graph(id="g-gender", config={"displayModeBar": False},
                            style={"height": "280px"})], span=6),
            card("Age Group Breakdown",
                 [dcc.Graph(id="g-age", config={"displayModeBar": False},
                            style={"height": "280px"})], span=6),
        ], style=grid("repeat(12, 1fr)")),

    ], style={"padding": "24px 32px", "background": C["bg"], "minHeight": "100vh"}),

], style={"background": C["bg"], "minHeight": "100vh"})


# -- CALLBACK --
@app.callback(
    Output("kpi-row",      "children"),
    Output("last-updated", "children"),
    Output("g-burnout",    "figure"),
    Output("g-overwork",   "figure"),
    Output("g-wellbeing",  "figure"),
    Output("g-workstress", "figure"),
    Output("g-sleepcat",   "figure"),
    Output("g-stresstier", "figure"),
    Output("g-gender",     "figure"),
    Output("g-age",        "figure"),
    Input("tick", "n_intervals"),
)
def update_all(_n):
    lf, pf, pr = load_all()
    now = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")

    has_lf = not lf.empty
    has_pf = not pf.empty
    has_pr = not pr.empty

    # -- KPIs --
    total_emp = pf["user_id"].nunique() if has_pf else (lf["user_id"].nunique() if has_lf else 0)
    avg_wb    = f"{lf['wellbeing_index'].mean():.1f}"    if (has_lf and 'wellbeing_index' in lf) else "-"
    avg_burn  = f"{pr['burnout_risk_index'].mean():.0f}" if (has_pr and 'burnout_risk_index' in pr) else "-"
    pct_over  = f"{pr['overwork_flag'].mean()*100:.0f}"  if (has_pr and 'overwork_flag' in pr) else "-"

    kpis = html.Div([
        kpi("Employees Tracked", f"{total_emp:,}", "people", C["cyan"]),
        kpi("Avg Wellbeing",     avg_wb,           "/ 10",   C["emerald"]),
        kpi("Avg Burnout Risk",  avg_burn,         "/ 100",  C["amber"]),
        kpi("Overworked",        pct_over,         "%",      C["rose"]),
    ], style={"display": "contents"})

    # -- 1. Burnout risk by industry --
    if has_pr and "burnout_risk_index" in pr.columns:
        burn = pr.groupby("industry")["burnout_risk_index"].mean().sort_values()
        colors_b = [C["rose"] if v >= 70 else C["amber"] if v >= 50
                    else C["emerald"] for v in burn.values]
        fig_burn = go.Figure(go.Bar(
            y=burn.index, x=burn.values, orientation="h",
            marker=dict(color=colors_b, line=dict(color=C["bg"], width=0.5)),
            text=[f"{v:.0f}" for v in burn.values], textposition="outside",
            textfont=dict(size=11, color=C["muted"]),
        ))
        fig_burn.update_layout(**BASE_LAYOUT)
        fig_burn.update_xaxes(range=[0, 105], title_text="Burnout index (0-100)")
    else:
        fig_burn = empty_fig()

    # -- 2. Overwork % by industry --
    if has_pr and "overwork_flag" in pr.columns:
        ow = pr.groupby("industry")["overwork_flag"].mean().mul(100).sort_values(ascending=False)
        fig_ow = go.Figure(go.Bar(
            x=ow.index, y=ow.values,
            marker=dict(color=[C["rose"] if v > 60 else C["amber"] if v > 30
                               else C["emerald"] for v in ow.values],
                        line=dict(color=C["bg"], width=0.5)),
            text=[f"{v:.0f}%" for v in ow.values], textposition="outside",
            textfont=dict(size=11, color=C["muted"]),
        ))
        fig_ow.update_layout(**BASE_LAYOUT)
        fig_ow.update_yaxes(range=[0, 110], title_text="% working >10 hrs/day")
        fig_ow.update_xaxes(tickangle=-25)
    else:
        fig_ow = empty_fig()

    # -- 3. Wellbeing index by industry (join lifestyle+profession) --
    if has_lf and has_pr and "wellbeing_index" in lf.columns:
        try:
            merged = pd.merge(
                lf[["user_id", "wellbeing_index"]],
                pr[["user_id", "industry"]],
                on="user_id", how="inner",
            )
            wb = merged.groupby("industry")["wellbeing_index"].mean().sort_values()
            fig_wb = go.Figure(go.Bar(
                y=wb.index, x=wb.values, orientation="h",
                marker=dict(color=[C["rose"] if v < 4 else C["amber"] if v < 6
                                   else C["emerald"] for v in wb.values],
                            line=dict(color=C["bg"], width=0.5)),
                text=[f"{v:.1f}" for v in wb.values], textposition="outside",
                textfont=dict(size=11, color=C["muted"]),
            ))
            fig_wb.update_layout(**BASE_LAYOUT)
            fig_wb.update_xaxes(range=[0, 10.5], title_text="Wellbeing index (0-10)")
        except Exception:
            fig_wb = empty_fig()
    else:
        fig_wb = empty_fig("Waiting for joined data ...")

    # -- 4. Work stress by industry --
    if has_pr and "work_stress_score" in pr.columns:
        ind = pr.groupby("industry")["work_stress_score"].mean().sort_values()
        fig_ws = go.Figure(go.Bar(
            y=ind.index, x=ind.values, orientation="h",
            marker=dict(color=[C["rose"] if v > 7 else C["amber"] if v > 5
                               else C["emerald"] for v in ind.values],
                        line=dict(color=C["bg"], width=0.5)),
            text=[f"{v:.1f}" for v in ind.values], textposition="outside",
            textfont=dict(size=11, color=C["muted"]),
        ))
        fig_ws.update_layout(**BASE_LAYOUT)
        fig_ws.update_xaxes(range=[0, 11], title_text="Avg work stress (0-10)")
    else:
        fig_ws = empty_fig()

    # -- 5. Sleep category donut --
    if has_lf and "sleep_category" in lf.columns:
        order = ["insufficient", "borderline", "optimal", "excessive"]
        sc = lf["sleep_category"].value_counts().reindex(order, fill_value=0)
        fig_sc = go.Figure(go.Pie(
            labels=sc.index, values=sc.values, hole=0.6,
            marker=dict(colors=[C["rose"], C["amber"], C["emerald"], C["violet"]],
                        line=dict(color=C["bg"], width=2)),
            textfont=dict(size=11, family="DM Mono"),
        ))
        fig_sc.update_layout(**BASE_LAYOUT)
    else:
        fig_sc = empty_fig()

    # -- 6. Stress tier distribution --
    if has_lf and "stress_tier" in lf.columns:
        order = ["low", "moderate", "high"]
        st = lf["stress_tier"].value_counts().reindex(order, fill_value=0)
        fig_st = go.Figure(go.Bar(
            x=st.index, y=st.values,
            marker=dict(color=[C["emerald"], C["amber"], C["rose"]],
                        line=dict(color=C["bg"], width=0.5)),
            text=st.values, textposition="outside",
            textfont=dict(size=11, color=C["muted"]),
        ))
        fig_st.update_layout(**BASE_LAYOUT)
        fig_st.update_yaxes(title_text="Records")
    else:
        fig_st = empty_fig()

    # -- 7. Gender distribution --
    if has_pf and "gender" in pf.columns:
        gc = pf["gender"].value_counts()
        fig_gen = go.Figure(go.Pie(
            labels=gc.index, values=gc.values, hole=0.55,
            marker=dict(colors=[C["cyan"], C["violet"], C["amber"]],
                        line=dict(color=C["bg"], width=2)),
            textfont=dict(size=11, family="DM Mono"),
        ))
        fig_gen.update_layout(**BASE_LAYOUT)
    else:
        fig_gen = empty_fig()

    # -- 8. Age group breakdown --
    if has_pf and "age_group" in pf.columns:
        order = ["18-24", "25-34", "35-49", "50-64", "65+"]
        ag = pf["age_group"].value_counts().reindex(order, fill_value=0)
        fig_age = go.Figure(go.Bar(
            x=ag.index, y=ag.values,
            marker=dict(color=C["violet"], line=dict(color=C["bg"], width=0.5)),
            text=ag.values, textposition="outside",
            textfont=dict(size=11, color=C["muted"]),
        ))
        fig_age.update_layout(**BASE_LAYOUT)
        fig_age.update_yaxes(title_text="Employees")
    else:
        fig_age = empty_fig()

    return (
        kpis, f"Last updated: {now}",
        fig_burn, fig_ow, fig_wb, fig_ws,
        fig_sc, fig_st, fig_gen, fig_age,
    )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8050)