"""
Claude Code Usage Analytics Dashboard — Streamlit Application.

A multi-section interactive dashboard presenting insights from
Claude Code telemetry data.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from src.db.repository import AnalyticsRepository
from src.analytics import core_metrics, user_patterns, tool_analysis, predictions

# ---------------------------------------------------------------------------
# Page Config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Claude Code Analytics",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    /* Dark modern theme overrides */
    .stApp {
        background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
    }

    .stMetric {
        background: linear-gradient(135deg, #1e1e3f 0%, #2d2d5e 100%);
        border: 1px solid rgba(139, 92, 246, 0.3);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 4px 20px rgba(139, 92, 246, 0.15);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .stMetric:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 30px rgba(139, 92, 246, 0.25);
    }

    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #a78bfa, #818cf8);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }

    [data-testid="stMetricLabel"] {
        font-size: 0.85rem;
        color: #a5b4fc;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    h1 {
        background: linear-gradient(135deg, #c084fc, #818cf8, #67e8f9);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
    }

    h2, h3 {
        color: #c4b5fd;
    }

    .stTabs [data-baseweb="tab-list"] {
        background: rgba(30, 30, 63, 0.7);
        border-radius: 12px;
        padding: 4px;
        gap: 4px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        color: #a5b4fc;
        font-weight: 500;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #4c1d95, #3730a3);
        color: #fff;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0f2e 0%, #1a1a3e 100%);
        border-right: 1px solid rgba(139, 92, 246, 0.2);
    }

    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }

    /* Section card style */
    .section-card {
        background: rgba(30, 30, 63, 0.5);
        border: 1px solid rgba(139, 92, 246, 0.15);
        border-radius: 16px;
        padding: 24px;
        margin-bottom: 16px;
        backdrop-filter: blur(10px);
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Color palette for charts
# ---------------------------------------------------------------------------

COLORS = {
    "primary": "#8b5cf6",
    "secondary": "#06b6d4",
    "accent": "#f59e0b",
    "success": "#10b981",
    "danger": "#ef4444",
    "info": "#3b82f6",
    "gradient": ["#8b5cf6", "#a78bfa", "#c4b5fd", "#818cf8", "#6366f1",
                  "#06b6d4", "#67e8f9", "#22d3ee", "#10b981", "#f59e0b"],
}

PLOTLY_TEMPLATE = "plotly_dark"

CHART_LAYOUT = dict(
    template=PLOTLY_TEMPLATE,
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#c4b5fd", family="Inter, sans-serif"),
    margin=dict(l=40, r=40, t=50, b=40),
    xaxis=dict(gridcolor="rgba(139, 92, 246, 0.1)"),
    yaxis=dict(gridcolor="rgba(139, 92, 246, 0.1)"),
)


def apply_chart_style(fig):
    """Apply consistent dark/gradient styling to a Plotly figure."""
    fig.update_layout(**CHART_LAYOUT)
    return fig


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar():
    with st.sidebar:
        st.markdown("# 🤖 Claude Code")
        st.markdown("### Analytics Dashboard")
        st.markdown("---")

        page = st.radio(
            "📊 Navigate",
            [
                "🏠 Overview",
                "💰 Cost & Token Analysis",
                "🔧 Tool Usage",
                "👥 User Patterns",
                "🔮 Predictions",
            ],
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown(
            "<div style='color: #6366f1; font-size: 0.75rem; text-align: center;'>"
            "Built with Streamlit + Plotly<br>"
            "Claude Code Telemetry Analytics"
            "</div>",
            unsafe_allow_html=True,
        )

    return page


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------

def render_overview(repo: AnalyticsRepository):
    st.markdown("# 🏠 Platform Overview")
    st.markdown("*High-level KPIs and activity summary across the organization*")

    kpis = core_metrics.get_overview_kpis(repo)

    # Top KPI cards
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Users", f"{kpis['total_users']}")
    col2.metric("Total Sessions", f"{kpis['total_sessions']:,}")
    col3.metric("Total Cost", f"${kpis['total_cost_usd']:,.2f}")
    col4.metric("API Requests", f"{kpis['total_api_requests']:,}")
    col5.metric("Error Rate", f"{kpis['error_rate']:.2f}%")

    st.markdown("")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Cost/Session", f"${kpis['avg_cost_per_session']:.2f}")
    col2.metric("Avg Cost/User", f"${kpis['avg_cost_per_user']:.2f}")
    col3.metric("Total Prompts", f"{kpis['total_prompts']:,}")
    col4.metric("Sessions/User", f"{kpis['avg_sessions_per_user']:.1f}")

    st.markdown("---")

    # Daily activity chart
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("### 📈 Daily Activity & Cost")
        df_costs = core_metrics.get_cost_trends(repo)
        if not df_costs.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_costs["date"], y=df_costs["request_count"],
                name="API Requests",
                marker_color=COLORS["primary"],
                opacity=0.6,
                yaxis="y2",
            ))
            fig.add_trace(go.Scatter(
                x=df_costs["date"], y=df_costs["total_cost"],
                name="Daily Cost ($)",
                line=dict(color=COLORS["secondary"], width=2),
                mode="lines+markers",
                marker=dict(size=4),
            ))
            fig.add_trace(go.Scatter(
                x=df_costs["date"], y=df_costs["cost_7d_ma"],
                name="7-Day Avg Cost",
                line=dict(color=COLORS["accent"], width=2, dash="dash"),
            ))
            fig.update_layout(
                yaxis=dict(title="Cost ($)", side="left"),
                yaxis2=dict(title="Request Count", overlaying="y", side="right"),
                legend=dict(orientation="h", y=1.12),
                height=400,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 🕐 Hourly Activity Heatmap")
        df_hourly = repo.get_hourly_activity()
        if not df_hourly.empty:
            fig = go.Figure(go.Bar(
                x=df_hourly["hour"],
                y=df_hourly["request_count"],
                marker=dict(
                    color=df_hourly["request_count"],
                    colorscale=[[0, "#1e1b4b"], [0.5, "#6366f1"], [1.0, "#c084fc"]],
                ),
            ))
            fig.update_layout(
                xaxis=dict(title="Hour of Day", tickmode="linear"),
                yaxis=dict(title="Requests"),
                height=400,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    # Day of week activity
    st.markdown("### 📅 Activity by Day of Week")
    df_dow = repo.get_daily_activity_by_dow()
    if not df_dow.empty:
        day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        df_dow["day_name"] = df_dow["day_of_week"].map(lambda x: day_names[int(x)])
        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                df_dow, x="day_name", y="request_count",
                color="request_count",
                color_continuous_scale=[[0, "#1e1b4b"], [1, "#8b5cf6"]],
            )
            fig.update_layout(
                xaxis_title="Day", yaxis_title="Requests",
                coloraxis_showscale=False, height=300,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                df_dow, x="day_name", y="total_cost",
                color="total_cost",
                color_continuous_scale=[[0, "#0c4a6e"], [1, "#06b6d4"]],
            )
            fig.update_layout(
                xaxis_title="Day", yaxis_title="Cost ($)",
                coloraxis_showscale=False, height=300,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Page: Cost & Token Analysis
# ---------------------------------------------------------------------------

def render_cost_analysis(repo: AnalyticsRepository):
    st.markdown("# 💰 Cost & Token Analysis")
    st.markdown("*Deep dive into token consumption, costs by model, practice, and level*")

    # Cost trends
    st.markdown("### 📊 Cost Trends Over Time")
    df_costs = core_metrics.get_cost_trends(repo)
    if not df_costs.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_costs["date"], y=df_costs["total_cost"],
            name="Daily Cost",
            fill="tozeroy",
            fillcolor="rgba(139, 92, 246, 0.15)",
            line=dict(color=COLORS["primary"], width=1.5),
        ))
        fig.add_trace(go.Scatter(
            x=df_costs["date"], y=df_costs["cost_7d_ma"],
            name="7-Day Moving Avg",
            line=dict(color=COLORS["accent"], width=2.5),
        ))
        fig.add_trace(go.Scatter(
            x=df_costs["date"], y=df_costs["cost_14d_ma"],
            name="14-Day Moving Avg",
            line=dict(color=COLORS["success"], width=2, dash="dot"),
        ))
        fig.update_layout(
            yaxis_title="Cost ($)",
            legend=dict(orientation="h", y=1.1),
            height=400,
        )
        apply_chart_style(fig)
        st.plotly_chart(fig, use_container_width=True)

    # Cumulative cost
    if not df_costs.empty:
        st.markdown("### 📈 Cumulative Cost")
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_costs["date"], y=df_costs["cumulative_cost"],
            fill="tozeroy",
            fillcolor="rgba(6, 182, 212, 0.15)",
            line=dict(color=COLORS["secondary"], width=2),
        ))
        fig.update_layout(yaxis_title="Cumulative Cost ($)", height=300)
        apply_chart_style(fig)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Model comparison
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🤖 Cost by Model")
        df_models = core_metrics.get_model_comparison(repo)
        if not df_models.empty:
            fig = px.pie(
                df_models, values="total_cost", names="model",
                color_discrete_sequence=COLORS["gradient"],
                hole=0.4,
            )
            fig.update_traces(
                textinfo="label+percent",
                textfont_size=11,
            )
            fig.update_layout(height=400, showlegend=False)
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 📋 Model Comparison Table")
        if not df_models.empty:
            display_df = df_models[[
                "model", "request_count", "total_cost",
                "avg_cost", "cost_share_pct", "cost_per_1k_tokens",
            ]].copy()
            display_df.columns = [
                "Model", "Requests", "Total Cost ($)",
                "Avg Cost ($)", "Cost Share (%)", "$/1K Tokens"
            ]
            display_df["Total Cost ($)"] = display_df["Total Cost ($)"].round(2)
            display_df["Avg Cost ($)"] = display_df["Avg Cost ($)"].round(4)
            st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    # Cost by practice and level
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🏢 Cost by Engineering Practice")
        df_practice = repo.get_cost_by_practice()
        if not df_practice.empty:
            fig = px.bar(
                df_practice, x="practice", y="total_cost",
                color="practice",
                color_discrete_sequence=COLORS["gradient"],
                text="total_cost",
            )
            fig.update_traces(texttemplate="$%{text:.0f}", textposition="outside")
            fig.update_layout(
                xaxis_title="", yaxis_title="Cost ($)",
                showlegend=False, height=400,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 📊 Cost by Seniority Level")
        df_level = user_patterns.get_level_comparison(repo)
        if not df_level.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_level["level"], y=df_level["total_cost"],
                name="Total Cost",
                marker_color=COLORS["primary"],
            ))
            fig.add_trace(go.Scatter(
                x=df_level["level"], y=df_level["cost_per_user"],
                name="Cost per User",
                yaxis="y2",
                line=dict(color=COLORS["accent"], width=3),
                mode="lines+markers",
                marker=dict(size=8),
            ))
            fig.update_layout(
                yaxis=dict(title="Total Cost ($)"),
                yaxis2=dict(title="Cost per User ($)", overlaying="y", side="right"),
                legend=dict(orientation="h", y=1.12),
                height=400,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    # Token analysis
    st.markdown("---")
    st.markdown("### 🔢 Token Consumption Trends")
    df_tokens = core_metrics.get_token_trends(repo)
    if not df_tokens.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_tokens["date"], y=df_tokens["total_input_tokens"],
            name="Input Tokens",
            marker_color=COLORS["primary"],
            opacity=0.7,
        ))
        fig.add_trace(go.Bar(
            x=df_tokens["date"], y=df_tokens["total_output_tokens"],
            name="Output Tokens",
            marker_color=COLORS["secondary"],
            opacity=0.7,
        ))
        fig.update_layout(
            barmode="stack",
            yaxis_title="Tokens",
            legend=dict(orientation="h", y=1.1),
            height=350,
        )
        apply_chart_style(fig)
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Page: Tool Usage
# ---------------------------------------------------------------------------

def render_tool_usage(repo: AnalyticsRepository):
    st.markdown("# 🔧 Tool Usage Analysis")
    st.markdown("*Which tools are used most, acceptance rates, success rates, and efficiency*")

    df_tools = tool_analysis.get_tool_overview(repo)

    if df_tools.empty:
        st.warning("No tool event data available.")
        return

    # Tool popularity
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("### 🏆 Tool Popularity")
        fig = px.bar(
            df_tools.head(15), x="total_events", y="tool_name",
            orientation="h",
            color="total_events",
            color_continuous_scale=[[0, "#1e1b4b"], [0.5, "#6366f1"], [1.0, "#c084fc"]],
        )
        fig.update_layout(
            yaxis=dict(categoryorder="total ascending"),
            xaxis_title="Total Events",
            yaxis_title="",
            coloraxis_showscale=False,
            height=500,
        )
        apply_chart_style(fig)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### ✅ Acceptance & Success Rates")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=df_tools["tool_name"], x=df_tools["acceptance_rate"],
            name="Acceptance %",
            orientation="h",
            marker_color=COLORS["success"],
            opacity=0.8,
        ))
        fig.add_trace(go.Bar(
            y=df_tools["tool_name"], x=df_tools["success_rate"],
            name="Success %",
            orientation="h",
            marker_color=COLORS["info"],
            opacity=0.8,
        ))
        fig.update_layout(
            barmode="group",
            xaxis_title="Rate (%)",
            yaxis=dict(categoryorder="total ascending"),
            yaxis_title="",
            legend=dict(orientation="h", y=1.08),
            height=500,
        )
        apply_chart_style(fig)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Tool efficiency
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### ⚡ Tool Execution Duration")
        df_eff = tool_analysis.get_tool_efficiency(repo)
        if not df_eff.empty:
            display = df_eff[df_eff["avg_duration_ms"] > 0].sort_values(
                "avg_duration_ms", ascending=True
            )
            fig = px.bar(
                display.head(15), x="avg_duration_ms", y="tool_name",
                orientation="h",
                color="avg_duration_ms",
                color_continuous_scale=[[0, "#064e3b"], [0.5, "#10b981"], [1.0, "#f59e0b"]],
            )
            fig.update_layout(
                xaxis_title="Avg Duration (ms)",
                yaxis_title="",
                coloraxis_showscale=False,
                height=400,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 📋 Decision Source Distribution")
        df_dec = tool_analysis.get_decision_source_distribution(repo)
        if not df_dec.empty:
            fig = px.sunburst(
                df_dec, path=["decision_source", "decision"],
                values="count",
                color_discrete_sequence=COLORS["gradient"],
            )
            fig.update_layout(height=400)
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    # Tool usage data table
    st.markdown("### 📊 Full Tool Metrics")
    display_df = df_tools[[
        "tool_name", "total_events", "decisions", "accepted",
        "rejected", "acceptance_rate", "success_rate",
    ]].copy()
    display_df.columns = [
        "Tool", "Total Events", "Decisions", "Accepted",
        "Rejected", "Accept Rate (%)", "Success Rate (%)",
    ]
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page: User Patterns
# ---------------------------------------------------------------------------

def render_user_patterns(repo: AnalyticsRepository):
    st.markdown("# 👥 User Patterns")
    st.markdown("*Who uses Claude Code, how intensively, and how is usage distributed*")

    dist = user_patterns.get_usage_distribution(repo)

    # Distribution KPIs
    if dist:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Active Users", f"{dist.get('active_users', 0)}")
        col2.metric("Gini Coefficient", f"{dist.get('gini_coefficient', 0):.3f}")
        col3.metric("Pareto (80% cost)", f"{dist.get('pareto_pct_users_for_80_cost', 0):.0f}% users")
        col4.metric("Avg Cost/Active User", f"${dist.get('avg_cost_per_active_user', 0):.2f}")

    st.markdown("---")

    # Power users
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("### 🏆 Top 10 Power Users")
        df_power = user_patterns.get_power_users(repo, top_n=10)
        if not df_power.empty:
            fig = px.bar(
                df_power, x="total_cost", y="full_name",
                orientation="h",
                color="practice",
                color_discrete_sequence=COLORS["gradient"],
                hover_data=["level", "session_count", "total_tokens"],
            )
            fig.update_layout(
                yaxis=dict(categoryorder="total ascending"),
                xaxis_title="Total Cost ($)",
                yaxis_title="",
                height=450,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 📊 Cost per User Distribution")
        df_users = repo.get_user_activity()
        if not df_users.empty:
            fig = px.histogram(
                df_users, x="total_cost", nbins=20,
                color_discrete_sequence=[COLORS["primary"]],
            )
            fig.update_layout(
                xaxis_title="Total Cost ($)",
                yaxis_title="Number of Users",
                height=450,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Practice and location comparison
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🏢 Usage by Practice")
        df_practice = user_patterns.get_practice_comparison(repo)
        if not df_practice.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_practice["practice"],
                y=df_practice["cost_per_user"],
                name="Cost per User",
                marker_color=COLORS["primary"],
            ))
            fig.add_trace(go.Bar(
                x=df_practice["practice"],
                y=df_practice["sessions_per_user"],
                name="Sessions per User",
                marker_color=COLORS["secondary"],
                yaxis="y2",
            ))
            fig.update_layout(
                yaxis=dict(title="Cost per User ($)"),
                yaxis2=dict(title="Sessions per User", overlaying="y", side="right"),
                legend=dict(orientation="h", y=1.12),
                height=400,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 🌍 Usage by Location")
        df_loc = user_patterns.get_location_comparison(repo)
        if not df_loc.empty:
            fig = px.bar(
                df_loc, x="location", y="total_cost",
                color="location",
                color_discrete_sequence=COLORS["gradient"],
                text="user_count",
            )
            fig.update_traces(
                texttemplate="%{text} users",
                textposition="outside",
            )
            fig.update_layout(
                xaxis_title="", yaxis_title="Total Cost ($)",
                showlegend=False, height=400,
            )
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    # Level comparison
    st.markdown("### 📊 Usage by Seniority Level")
    df_level = user_patterns.get_level_comparison(repo)
    if not df_level.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_level["level"], y=df_level["request_count"],
            name="Request Count",
            marker_color=COLORS["primary"],
            opacity=0.7,
        ))
        fig.add_trace(go.Scatter(
            x=df_level["level"], y=df_level["cost_per_user"],
            name="Cost per User ($)",
            yaxis="y2",
            line=dict(color=COLORS["accent"], width=3),
            mode="lines+markers",
            marker=dict(size=10),
        ))
        fig.update_layout(
            yaxis=dict(title="Request Count"),
            yaxis2=dict(title="Cost per User ($)", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
            height=350,
        )
        apply_chart_style(fig)
        st.plotly_chart(fig, use_container_width=True)

    # Full user table
    st.markdown("### 📋 All Users")
    df_users = repo.get_user_activity()
    if not df_users.empty:
        display = df_users[[
            "full_name", "email", "practice", "level", "location",
            "session_count", "total_cost", "total_events",
        ]].copy()
        display.columns = [
            "Name", "Email", "Practice", "Level", "Location",
            "Sessions", "Cost ($)", "Events",
        ]
        display["Cost ($)"] = display["Cost ($)"].round(2)
        st.dataframe(display, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page: Predictions
# ---------------------------------------------------------------------------

def render_predictions(repo: AnalyticsRepository):
    st.markdown("# 🔮 Predictive Analytics")
    st.markdown("*Cost forecasting, anomaly detection, and growth projections*")

    # Growth stats
    growth = predictions.forecast_usage_growth(repo)
    if growth:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Daily Avg Cost", f"${growth.get('daily_avg_cost', 0):.2f}")
        col2.metric("Projected Monthly", f"${growth.get('projected_monthly_cost', 0):.2f}")
        col3.metric(
            "Growth Rate",
            f"{growth.get('growth_rate_pct', 0):+.1f}%",
            delta=f"{growth.get('growth_rate_pct', 0):+.1f}%",
        )
        col4.metric("Weekly Avg Cost", f"${growth.get('weekly_avg_cost', 0):.2f}")

    st.markdown("---")

    # Forecast chart
    st.markdown("### 📈 Cost Forecast")
    forecast_days = st.slider("Forecast horizon (days)", 7, 30, 14)

    # Try Prophet first, fall back to polynomial
    df_forecast = predictions.try_prophet_forecast(repo, forecast_days)
    if df_forecast is None:
        df_forecast = predictions.forecast_costs(repo, forecast_days)

    if not df_forecast.empty:
        fig = go.Figure()

        # Actual data
        actual = df_forecast[~df_forecast["is_forecast"]]
        fig.add_trace(go.Scatter(
            x=actual["ds"], y=actual["y"],
            name="Actual Cost",
            mode="lines+markers",
            line=dict(color=COLORS["primary"], width=2),
            marker=dict(size=4),
        ))

        # Forecast
        forecast = df_forecast[df_forecast["is_forecast"]]
        fig.add_trace(go.Scatter(
            x=forecast["ds"], y=forecast["yhat"],
            name="Forecast",
            mode="lines",
            line=dict(color=COLORS["accent"], width=2, dash="dash"),
        ))

        # Confidence interval
        fig.add_trace(go.Scatter(
            x=pd.concat([forecast["ds"], forecast["ds"].iloc[::-1]]),
            y=pd.concat([forecast["yhat_upper"], forecast["yhat_lower"].iloc[::-1]]),
            fill="toself",
            fillcolor="rgba(245, 158, 11, 0.15)",
            line=dict(color="rgba(0,0,0,0)"),
            name="95% Confidence",
        ))

        fig.update_layout(
            yaxis_title="Daily Cost ($)",
            legend=dict(orientation="h", y=1.1),
            height=450,
        )
        apply_chart_style(fig)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Insufficient data for forecasting (need at least 7 days).")

    st.markdown("---")

    # Anomaly detection
    st.markdown("### 🔍 Anomaly Detection")
    z_threshold = st.slider("Anomaly sensitivity (z-score threshold)", 1.5, 3.5, 2.0, 0.1)

    df_anomalies = predictions.detect_anomalies(repo, z_threshold=z_threshold)
    if not df_anomalies.empty:
        fig = go.Figure()

        # Normal data
        normal = df_anomalies[~df_anomalies["is_anomaly"]]
        fig.add_trace(go.Scatter(
            x=normal["ds"], y=normal["y"],
            name="Normal",
            mode="markers",
            marker=dict(color=COLORS["primary"], size=6),
        ))

        # Anomalies
        anomalous = df_anomalies[df_anomalies["is_anomaly"]]
        if not anomalous.empty:
            spikes = anomalous[anomalous["anomaly_type"] == "spike"]
            drops = anomalous[anomalous["anomaly_type"] == "drop"]

            if not spikes.empty:
                fig.add_trace(go.Scatter(
                    x=spikes["ds"], y=spikes["y"],
                    name="Cost Spike",
                    mode="markers",
                    marker=dict(color=COLORS["danger"], size=12, symbol="triangle-up"),
                ))
            if not drops.empty:
                fig.add_trace(go.Scatter(
                    x=drops["ds"], y=drops["y"],
                    name="Cost Drop",
                    mode="markers",
                    marker=dict(color=COLORS["success"], size=12, symbol="triangle-down"),
                ))

        # Rolling mean
        fig.add_trace(go.Scatter(
            x=df_anomalies["ds"], y=df_anomalies["rolling_mean"],
            name="Rolling Mean",
            line=dict(color=COLORS["secondary"], width=2, dash="dash"),
        ))

        anomaly_count = int(df_anomalies["is_anomaly"].sum())
        fig.update_layout(
            title=f"Detected {anomaly_count} anomalous day(s)",
            yaxis_title="Daily Cost ($)",
            legend=dict(orientation="h", y=1.15),
            height=400,
        )
        apply_chart_style(fig)
        st.plotly_chart(fig, use_container_width=True)

        # Anomaly details
        if not anomalous.empty:
            st.markdown("#### 📋 Anomaly Details")
            detail_df = anomalous[["ds", "y", "rolling_mean", "z_score", "anomaly_type"]].copy()
            detail_df.columns = ["Date", "Cost ($)", "Rolling Mean ($)", "Z-Score", "Type"]
            detail_df["Cost ($)"] = detail_df["Cost ($)"].round(2)
            detail_df["Rolling Mean ($)"] = detail_df["Rolling Mean ($)"].round(2)
            detail_df["Z-Score"] = detail_df["Z-Score"].round(2)
            st.dataframe(detail_df, use_container_width=True, hide_index=True)
    else:
        st.info("Insufficient data for anomaly detection.")

    # Error analysis
    st.markdown("---")
    st.markdown("### ⚠️ Error Analysis")
    df_errors = repo.get_error_summary()
    if not df_errors.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.pie(
                df_errors, values="error_count", names="error",
                color_discrete_sequence=COLORS["gradient"],
                hole=0.4,
            )
            fig.update_traces(textinfo="label+percent", textfont_size=10)
            fig.update_layout(height=350, showlegend=False)
            apply_chart_style(fig)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.dataframe(
                df_errors.rename(columns={
                    "error": "Error", "status_code": "Status", "error_count": "Count"
                }),
                use_container_width=True,
                hide_index=True,
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Initialize repository
    repo = AnalyticsRepository()

    # Render sidebar and get selected page
    page = render_sidebar()

    # Render selected page
    if page == "🏠 Overview":
        render_overview(repo)
    elif page == "💰 Cost & Token Analysis":
        render_cost_analysis(repo)
    elif page == "🔧 Tool Usage":
        render_tool_usage(repo)
    elif page == "👥 User Patterns":
        render_user_patterns(repo)
    elif page == "🔮 Predictions":
        render_predictions(repo)


if __name__ == "__main__":
    main()
