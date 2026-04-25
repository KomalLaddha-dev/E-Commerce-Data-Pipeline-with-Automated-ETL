"""
app.py — E-Commerce Business Intelligence Dashboard
E-commerce Data Pipeline with Automated ETL

Advanced 8-page analytics dashboard built with Streamlit and Plotly.
Provides actionable business intelligence, not just charts.

Pages:
  1. Executive Summary
  2. Revenue Analytics
  3. Customer Intelligence
  4. Retention & Churn
  5. Conversion & Orders
  6. Product Performance
  7. Payment Analytics
  8. Actionable Insights
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ──────────────────────────────────────────────
# Page Configuration
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────
# Custom CSS
# ──────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 16px;
        padding: 1.5rem;
        color: white;
        text-align: center;
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        margin: 0.3rem 0;
        background: linear-gradient(90deg, #00d2ff, #3a7bd5);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #a0aec0;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 500;
    }
    .kpi-delta-positive {
        color: #48bb78;
        font-size: 0.9rem;
        font-weight: 600;
    }
    .kpi-delta-negative {
        color: #fc8181;
        font-size: 0.9rem;
        font-weight: 600;
    }

    /* Insight Box */
    .insight-box {
        background: linear-gradient(135deg, #0f2027 0%, #203a43 50%, #2c5364 100%);
        border-left: 4px solid #3a7bd5;
        border-radius: 8px;
        padding: 1.2rem 1.5rem;
        margin: 0.8rem 0;
        color: #e2e8f0;
    }
    .insight-box h4 {
        color: #63b3ed;
        margin-bottom: 0.3rem;
        font-size: 1rem;
    }

    /* Section Headers */
    .section-header {
        font-size: 1.3rem;
        font-weight: 600;
        color: #e2e8f0;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid rgba(99,179,237,0.3);
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
    }
    [data-testid="stSidebar"] .css-1d391kg {
        padding-top: 2rem;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# Database Connection
# ──────────────────────────────────────────────
@st.cache_resource
def get_engine():
    """Create a cached database engine."""
    conn_str = (
        f"{os.environ.get('DW_DRIVER', 'postgresql')}://"
        f"{os.environ.get('DW_USER', 'etl_user')}:"
        f"{os.environ.get('DW_PASSWORD', 'etl_password')}@"
        f"{os.environ.get('DW_HOST', 'localhost')}:"
        f"{os.environ.get('DW_PORT', '5433')}/"
        f"{os.environ.get('DW_NAME', 'ecommerce_dw')}"
    )
    return create_engine(conn_str, pool_pre_ping=True)


@st.cache_data(ttl=300)
def run_query(query, _engine=None):
    """Run a SQL query and return a DataFrame, cached for 5 minutes."""
    engine = _engine or get_engine()
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def format_inr(value):
    """Format a number as Indian Rupees."""
    if pd.isna(value) or value == 0:
        return "₹0"
    if abs(value) >= 1e7:
        return f"₹{value/1e7:.2f} Cr"
    elif abs(value) >= 1e5:
        return f"₹{value/1e5:.2f} L"
    elif abs(value) >= 1e3:
        return f"₹{value/1e3:.1f}K"
    return f"₹{value:,.0f}"


def kpi_card(label, value, delta=None, delta_label=""):
    """Render a styled KPI card."""
    delta_html = ""
    if delta is not None:
        cls = "kpi-delta-positive" if delta >= 0 else "kpi-delta-negative"
        arrow = "▲" if delta >= 0 else "▼"
        delta_html = f'<div class="{cls}">{arrow} {abs(delta):.1f}% {delta_label}</div>'

    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def insight_box(title, content):
    """Render a styled insight box."""
    st.markdown(f"""
    <div class="insight-box">
        <h4>💡 {title}</h4>
        <p>{content}</p>
    </div>
    """, unsafe_allow_html=True)


PLOTLY_THEME = {
    "template": "plotly_dark",
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "font": {"family": "Inter", "size": 12},
    "margin": {"l": 40, "r": 20, "t": 40, "b": 40},
}

COLORS = ["#3a7bd5", "#00d2ff", "#48bb78", "#ed8936", "#fc8181",
          "#9f7aea", "#f6e05e", "#ed64a6", "#4fd1c5", "#667eea"]


# ══════════════════════════════════════════════
# PAGE 1: EXECUTIVE SUMMARY
# ══════════════════════════════════════════════
def page_executive_summary():
    st.markdown("## 📊 Executive Summary")

    engine = get_engine()

    # KPIs
    kpis = run_query("""
        SELECT
            COUNT(DISTINCT order_id) as total_orders,
            SUM(net_amount) as total_revenue,
            SUM(gross_profit) as total_profit,
            ROUND(AVG(net_amount), 2) as avg_line_value,
            COUNT(DISTINCT customer_key) as unique_customers,
            SUM(quantity) as total_items
        FROM fact_order_items
        WHERE order_status NOT IN ('cancelled', 'returned')
    """, engine)

    customer_count = run_query("SELECT COUNT(*) as cnt FROM dim_customer", engine)
    product_count = run_query("SELECT COUNT(*) as cnt FROM dim_product", engine)

    if not kpis.empty:
        row = kpis.iloc[0]

        # Monthly comparison for deltas
        monthly = run_query("""
            SELECT d.year, d.month,
                   SUM(f.net_amount) as revenue,
                   COUNT(DISTINCT f.order_id) as orders
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY d.year, d.month
            ORDER BY d.year DESC, d.month DESC
            LIMIT 2
        """, engine)

        rev_delta = None
        if len(monthly) >= 2:
            rev_delta = ((monthly.iloc[0]["revenue"] - monthly.iloc[1]["revenue"])
                         / monthly.iloc[1]["revenue"] * 100)

        cols = st.columns(5)
        with cols[0]:
            kpi_card("Total Revenue", format_inr(row["total_revenue"]), rev_delta, "MoM")
        with cols[1]:
            kpi_card("Gross Profit", format_inr(row["total_profit"]))
        with cols[2]:
            kpi_card("Total Orders", f"{int(row['total_orders']):,}")
        with cols[3]:
            kpi_card("Unique Customers", f"{int(row['unique_customers']):,}")
        with cols[4]:
            kpi_card("Avg Line Value", format_inr(row["avg_line_value"]))

    st.markdown("---")

    # Revenue Trend + Order Volume
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-header">📈 Monthly Revenue Trend</div>', unsafe_allow_html=True)
        monthly_rev = run_query("""
            SELECT d.year, d.month, d.month_name,
                   SUM(f.net_amount) as revenue,
                   SUM(f.gross_profit) as profit,
                   COUNT(DISTINCT f.order_id) as orders
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month
        """, engine)
        if not monthly_rev.empty:
            monthly_rev["period"] = monthly_rev["month_name"].str[:3] + " " + monthly_rev["year"].astype(str)
            fig = go.Figure()
            fig.add_trace(go.Bar(x=monthly_rev["period"], y=monthly_rev["revenue"],
                                 name="Revenue", marker_color="#3a7bd5", opacity=0.8))
            fig.add_trace(go.Scatter(x=monthly_rev["period"], y=monthly_rev["profit"],
                                     name="Profit", line=dict(color="#48bb78", width=3), yaxis="y"))
            fig.update_layout(**PLOTLY_THEME, height=350, showlegend=True,
                              legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">🛒 Category Revenue Split</div>', unsafe_allow_html=True)
        categories = run_query("""
            SELECT p.category,
                   SUM(f.net_amount) as revenue,
                   SUM(f.gross_profit) as profit,
                   COUNT(DISTINCT f.order_id) as orders
            FROM fact_order_items f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY p.category ORDER BY revenue DESC
        """, engine)
        if not categories.empty:
            fig = px.pie(categories, names="category", values="revenue",
                         color_discrete_sequence=COLORS, hole=0.4)
            fig.update_layout(**PLOTLY_THEME, height=350, showlegend=True,
                              legend=dict(orientation="h", y=-0.1))
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

    # Customer Segments + Geographic
    col3, col4 = st.columns(2)

    with col3:
        st.markdown('<div class="section-header">👥 Customer Segments</div>', unsafe_allow_html=True)
        segments = run_query("""
            SELECT c.segment,
                   COUNT(DISTINCT c.customer_key) as customers,
                   COALESCE(SUM(f.net_amount), 0) as revenue,
                   COALESCE(COUNT(DISTINCT f.order_id), 0) as orders
            FROM dim_customer c
            LEFT JOIN fact_order_items f ON c.customer_key = f.customer_key
                AND f.order_status NOT IN ('cancelled','returned')
            GROUP BY c.segment ORDER BY revenue DESC
        """, engine)
        if not segments.empty:
            fig = px.bar(segments, x="segment", y=["revenue", "customers"],
                         barmode="group", color_discrete_sequence=["#3a7bd5", "#00d2ff"])
            fig.update_layout(**PLOTLY_THEME, height=300, showlegend=True,
                              legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.markdown('<div class="section-header">🗺 Top Cities by Revenue</div>', unsafe_allow_html=True)
        geo = run_query("""
            SELECT l.city, l.state, l.region,
                   SUM(f.net_amount) as revenue,
                   COUNT(DISTINCT f.order_id) as orders
            FROM fact_order_items f
            JOIN dim_location l ON f.location_key = l.location_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY l.city, l.state, l.region
            ORDER BY revenue DESC LIMIT 10
        """, engine)
        if not geo.empty:
            fig = px.bar(geo, x="revenue", y="city", orientation="h",
                         color="region", color_discrete_sequence=COLORS)
            fig.update_layout(**PLOTLY_THEME, height=300, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════
# PAGE 2: REVENUE ANALYTICS
# ══════════════════════════════════════════════
def page_revenue_analytics():
    st.markdown("## 📈 Revenue Analytics")
    engine = get_engine()

    # Time Filter
    time_filter = st.radio("Time Granularity", ["Daily", "Weekly", "Monthly", "Quarterly"],
                           horizontal=True, index=2)

    if time_filter == "Daily":
        data = run_query("""
            SELECT d.full_date::text as period,
                   SUM(f.net_amount) as revenue,
                   SUM(f.gross_profit) as profit,
                   COUNT(DISTINCT f.order_id) as orders,
                   COUNT(DISTINCT f.customer_key) as customers
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY d.full_date ORDER BY d.full_date
        """, engine)
    elif time_filter == "Weekly":
        data = run_query("""
            SELECT d.year || '-W' || LPAD(d.week_of_year::text, 2, '0') as period,
                   SUM(f.net_amount) as revenue,
                   SUM(f.gross_profit) as profit,
                   COUNT(DISTINCT f.order_id) as orders,
                   COUNT(DISTINCT f.customer_key) as customers
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY d.year, d.week_of_year
            ORDER BY d.year, d.week_of_year
        """, engine)
    elif time_filter == "Quarterly":
        data = run_query("""
            SELECT 'Q' || d.quarter || ' ' || d.year as period,
                   SUM(f.net_amount) as revenue,
                   SUM(f.gross_profit) as profit,
                   COUNT(DISTINCT f.order_id) as orders,
                   COUNT(DISTINCT f.customer_key) as customers
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY d.year, d.quarter
            ORDER BY d.year, d.quarter
        """, engine)
    else:
        data = run_query("""
            SELECT d.month_name || ' ' || d.year as period,
                   SUM(f.net_amount) as revenue,
                   SUM(f.gross_profit) as profit,
                   COUNT(DISTINCT f.order_id) as orders,
                   COUNT(DISTINCT f.customer_key) as customers
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month
        """, engine)

    if not data.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=data["period"], y=data["revenue"], name="Revenue",
                             marker_color="#3a7bd5", opacity=0.8), secondary_y=False)
        fig.add_trace(go.Scatter(x=data["period"], y=data["orders"], name="Orders",
                                 line=dict(color="#ed8936", width=2)), secondary_y=True)
        fig.update_layout(**PLOTLY_THEME, height=400)
        fig.update_yaxes(title_text="Revenue (₹)", secondary_y=False)
        fig.update_yaxes(title_text="Orders", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    # Weekend vs Weekday
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-header">📅 Weekend vs Weekday</div>', unsafe_allow_html=True)
        weekend = run_query("""
            SELECT CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
                   SUM(f.net_amount) as revenue,
                   COUNT(DISTINCT f.order_id) as orders,
                   ROUND(AVG(f.net_amount), 2) as avg_value
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END
        """, engine)
        if not weekend.empty:
            fig = px.bar(weekend, x="day_type", y="revenue", color="day_type",
                         color_discrete_sequence=["#3a7bd5", "#00d2ff"])
            fig.update_layout(**PLOTLY_THEME, height=300, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">📊 Revenue by Day of Week</div>', unsafe_allow_html=True)
        dow = run_query("""
            SELECT d.day_name,
                   d.day_of_week,
                   SUM(f.net_amount) as revenue
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY d.day_name, d.day_of_week
            ORDER BY d.day_of_week
        """, engine)
        if not dow.empty:
            fig = px.bar(dow, x="day_name", y="revenue",
                         color_discrete_sequence=["#3a7bd5"])
            fig.update_layout(**PLOTLY_THEME, height=300)
            st.plotly_chart(fig, use_container_width=True)

    # Profit Margin by Category
    st.markdown('<div class="section-header">💰 Profit Margin by Category</div>', unsafe_allow_html=True)
    margins = run_query("""
        SELECT p.category,
               SUM(f.net_amount) as revenue,
               SUM(f.gross_profit) as profit,
               ROUND(SUM(f.gross_profit) * 100.0 / NULLIF(SUM(f.net_amount), 0), 1) as margin_pct
        FROM fact_order_items f
        JOIN dim_product p ON f.product_key = p.product_key
        WHERE f.order_status NOT IN ('cancelled','returned')
        GROUP BY p.category ORDER BY revenue DESC
    """, engine)
    if not margins.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=margins["category"], y=margins["revenue"],
                             name="Revenue", marker_color="#3a7bd5"))
        fig.add_trace(go.Bar(x=margins["category"], y=margins["profit"],
                             name="Profit", marker_color="#48bb78"))
        fig.update_layout(**PLOTLY_THEME, height=350, barmode="overlay",
                          legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig, use_container_width=True)

        # Insight
        top_margin = margins.loc[margins["margin_pct"].idxmax()]
        low_margin = margins.loc[margins["margin_pct"].idxmin()]
        insight_box("Margin Opportunities",
                    f"<b>{top_margin['category']}</b> has the highest margin at {top_margin['margin_pct']}%, "
                    f"while <b>{low_margin['category']}</b> has the lowest at {low_margin['margin_pct']}%. "
                    f"Consider increasing promotion of high-margin categories to optimize profitability.")


# ══════════════════════════════════════════════
# PAGE 3: CUSTOMER INTELLIGENCE
# ══════════════════════════════════════════════
def page_customer_intelligence():
    st.markdown("## 👥 Customer Intelligence")
    engine = get_engine()

    # Segment KPIs
    seg_data = run_query("""
        SELECT c.segment,
               COUNT(DISTINCT c.customer_key) as customers,
               COALESCE(SUM(f.net_amount), 0) as revenue,
               COALESCE(COUNT(DISTINCT f.order_id), 0) as orders,
               COALESCE(ROUND(AVG(f.net_amount), 2), 0) as avg_value
        FROM dim_customer c
        LEFT JOIN fact_order_items f ON c.customer_key = f.customer_key
            AND f.order_status NOT IN ('cancelled','returned')
        GROUP BY c.segment ORDER BY revenue DESC
    """, engine)

    if not seg_data.empty:
        cols = st.columns(len(seg_data))
        icons = {"Premium": "🌟", "Regular": "👤", "New": "🆕"}
        for i, row in seg_data.iterrows():
            with cols[i]:
                icon = icons.get(row["segment"], "👤")
                kpi_card(
                    f"{icon} {row['segment']} Customers",
                    f"{int(row['customers']):,}",
                )
                st.caption(f"Revenue: {format_inr(row['revenue'])} | Orders: {int(row['orders']):,}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-header">📊 Revenue by Segment</div>', unsafe_allow_html=True)
        if not seg_data.empty:
            fig = px.pie(seg_data, names="segment", values="revenue",
                         color_discrete_sequence=["#3a7bd5", "#48bb78", "#ed8936"], hole=0.45)
            fig.update_layout(**PLOTLY_THEME, height=350)
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">📈 CLV Distribution</div>', unsafe_allow_html=True)
        clv = run_query("""
            SELECT c.segment,
                   SUM(f.net_amount) as lifetime_value
            FROM fact_order_items f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY c.customer_key, c.segment
        """, engine)
        if not clv.empty:
            fig = px.histogram(clv, x="lifetime_value", color="segment",
                               nbins=30, color_discrete_sequence=["#3a7bd5", "#48bb78", "#ed8936"],
                               barmode="overlay", opacity=0.7)
            fig.update_layout(**PLOTLY_THEME, height=350,
                              xaxis_title="Lifetime Value (₹)", yaxis_title="Customer Count")
            st.plotly_chart(fig, use_container_width=True)

    # Top Customers
    st.markdown('<div class="section-header">🏆 Top 15 Customers by Lifetime Value</div>', unsafe_allow_html=True)
    top_cust = run_query("""
        SELECT c.customer_id,
               c.first_name || ' ' || c.last_name as name,
               c.city, c.segment,
               COUNT(DISTINCT f.order_id) as orders,
               SUM(f.net_amount) as lifetime_value,
               MIN(d.full_date)::text as first_order,
               MAX(d.full_date)::text as last_order
        FROM fact_order_items f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE f.order_status NOT IN ('cancelled','returned')
        GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.segment
        ORDER BY lifetime_value DESC LIMIT 15
    """, engine)
    if not top_cust.empty:
        st.dataframe(
            top_cust.style.format({"lifetime_value": "₹{:,.0f}"}),
            use_container_width=True, height=400
        )

    # Insight
    if not seg_data.empty:
        premium = seg_data[seg_data["segment"] == "Premium"]
        new = seg_data[seg_data["segment"] == "New"]
        if not premium.empty and not new.empty:
            premium_rppc = premium.iloc[0]["revenue"] / max(premium.iloc[0]["customers"], 1)
            new_rppc = new.iloc[0]["revenue"] / max(new.iloc[0]["customers"], 1)
            insight_box("How to Upgrade New → Premium",
                        f"Premium customers spend <b>{format_inr(premium_rppc)}</b> per person vs "
                        f"<b>{format_inr(new_rppc)}</b> for New customers ({premium_rppc/max(new_rppc,1):.1f}x more). "
                        f"Target the top {int(new.iloc[0]['customers'] * 0.1)} New customers with personalized "
                        f"offers on high-margin categories to accelerate segment upgrades.")


# ══════════════════════════════════════════════
# PAGE 4: RETENTION & CHURN
# ══════════════════════════════════════════════
def page_retention_churn():
    st.markdown("## 🔄 Retention & Churn Analysis")
    engine = get_engine()

    # Repeat Purchase Rate
    repeat_data = run_query("""
        WITH customer_orders AS (
            SELECT customer_key, COUNT(DISTINCT order_id) as order_count
            FROM fact_order_items
            WHERE order_status NOT IN ('cancelled','returned')
            GROUP BY customer_key
        )
        SELECT
            CASE
                WHEN order_count = 1 THEN '1 (One-time)'
                WHEN order_count BETWEEN 2 AND 3 THEN '2-3 (Returning)'
                WHEN order_count BETWEEN 4 AND 7 THEN '4-7 (Loyal)'
                ELSE '8+ (VIP)'
            END as frequency_band,
            COUNT(*) as customer_count
        FROM customer_orders
        GROUP BY CASE
            WHEN order_count = 1 THEN '1 (One-time)'
            WHEN order_count BETWEEN 2 AND 3 THEN '2-3 (Returning)'
            WHEN order_count BETWEEN 4 AND 7 THEN '4-7 (Loyal)'
            ELSE '8+ (VIP)'
        END
        ORDER BY MIN(order_count)
    """, engine)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-header">📊 Purchase Frequency Distribution</div>', unsafe_allow_html=True)
        if not repeat_data.empty:
            fig = px.pie(repeat_data, names="frequency_band", values="customer_count",
                         color_discrete_sequence=COLORS, hole=0.4)
            fig.update_layout(**PLOTLY_THEME, height=350)
            st.plotly_chart(fig, use_container_width=True)

            total = repeat_data["customer_count"].sum()
            one_time = repeat_data[repeat_data["frequency_band"].str.startswith("1")]["customer_count"].sum()
            repeat_rate = (1 - one_time / total) * 100 if total > 0 else 0
            kpi_card("Repeat Purchase Rate", f"{repeat_rate:.1f}%")

    with col2:
        st.markdown('<div class="section-header">📅 Monthly Active Customers</div>', unsafe_allow_html=True)
        mac = run_query("""
            SELECT d.year, d.month,
                   d.month_name || ' ' || d.year as period,
                   COUNT(DISTINCT f.customer_key) as active_customers,
                   COUNT(DISTINCT f.order_id) as orders
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month
        """, engine)
        if not mac.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=mac["period"], y=mac["active_customers"],
                                     fill="tozeroy", fillcolor="rgba(58,123,213,0.3)",
                                     line=dict(color="#3a7bd5", width=2),
                                     name="Active Customers"))
            fig.update_layout(**PLOTLY_THEME, height=350)
            st.plotly_chart(fig, use_container_width=True)

    # Cohort Analysis
    st.markdown('<div class="section-header">📊 Monthly Cohort Retention</div>', unsafe_allow_html=True)
    cohort = run_query("""
        WITH first_purchase AS (
            SELECT customer_key,
                   MIN(d.year * 100 + d.month) as cohort_month
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY customer_key
        ),
        customer_activity AS (
            SELECT f.customer_key,
                   fp.cohort_month,
                   d.year * 100 + d.month as activity_month
            FROM fact_order_items f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN first_purchase fp ON f.customer_key = fp.customer_key
            WHERE f.order_status NOT IN ('cancelled','returned')
        )
        SELECT cohort_month, activity_month,
               COUNT(DISTINCT customer_key) as customers
        FROM customer_activity
        GROUP BY cohort_month, activity_month
        ORDER BY cohort_month, activity_month
    """, engine)

    if not cohort.empty:
        # Build cohort pivot
        cohort["months_since"] = ((cohort["activity_month"] % 100 + cohort["activity_month"] // 100 * 12)
                                   - (cohort["cohort_month"] % 100 + cohort["cohort_month"] // 100 * 12))
        cohort_pivot = cohort.pivot_table(index="cohort_month", columns="months_since",
                                          values="customers", fill_value=0)

        # Normalize to retention %
        cohort_pct = cohort_pivot.div(cohort_pivot.iloc[:, 0], axis=0) * 100

        # Show top 8 cohorts
        if len(cohort_pct) > 8:
            cohort_pct = cohort_pct.tail(8)
        if len(cohort_pct.columns) > 8:
            cohort_pct = cohort_pct.iloc[:, :8]

        fig = px.imshow(cohort_pct, text_auto=".0f", aspect="auto",
                        color_continuous_scale="Blues",
                        labels=dict(x="Months Since First Purchase", y="Cohort", color="Retention %"))
        fig.update_layout(**PLOTLY_THEME, height=350)
        st.plotly_chart(fig, use_container_width=True)

    # Churn Insight
    if not repeat_data.empty:
        insight_box("Churn Reduction Strategy",
                    f"<b>{one_time:,}</b> customers ({100 - repeat_rate:.1f}%) made only one purchase. "
                    f"Implement a '7-day post-purchase' email sequence with personalized recommendations "
                    f"to convert one-time buyers into repeat customers. Even a 5% conversion would add "
                    f"<b>{int(one_time * 0.05):,}</b> returning customers.")


# ══════════════════════════════════════════════
# PAGE 5: CONVERSION & ORDERS
# ══════════════════════════════════════════════
def page_conversion():
    st.markdown("## 🛒 Conversion & Order Analysis")
    engine = get_engine()

    # Order Status Distribution
    status = run_query("""
        SELECT order_status,
               COUNT(DISTINCT order_id) as orders,
               SUM(net_amount) as value
        FROM fact_order_items
        GROUP BY order_status
        ORDER BY orders DESC
    """, engine)

    if not status.empty:
        total_orders = status["orders"].sum()
        delivered = status[status["order_status"] == "delivered"]["orders"].sum()
        cancelled = status[status["order_status"] == "cancelled"]["orders"].sum()
        returned = status[status["order_status"] == "returned"]["orders"].sum()

        cols = st.columns(4)
        with cols[0]:
            kpi_card("Total Orders", f"{total_orders:,}")
        with cols[1]:
            kpi_card("Delivered", f"{delivered:,}",
                     delivered / total_orders * 100 if total_orders > 0 else 0, "of total")
        with cols[2]:
            cancel_rate = cancelled / total_orders * 100 if total_orders > 0 else 0
            kpi_card("Cancellation Rate", f"{cancel_rate:.1f}%")
        with cols[3]:
            return_rate = returned / total_orders * 100 if total_orders > 0 else 0
            kpi_card("Return Rate", f"{return_rate:.1f}%")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-header">📊 Order Status Funnel</div>', unsafe_allow_html=True)
        if not status.empty:
            fig = px.funnel(status.sort_values("orders", ascending=False),
                            x="orders", y="order_status",
                            color_discrete_sequence=COLORS)
            fig.update_layout(**PLOTLY_THEME, height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">📦 Items per Order Distribution</div>', unsafe_allow_html=True)
        items_per_order = run_query("""
            SELECT order_id, COUNT(*) as item_count
            FROM fact_order_items
            WHERE order_status NOT IN ('cancelled','returned')
            GROUP BY order_id
        """, engine)
        if not items_per_order.empty:
            fig = px.histogram(items_per_order, x="item_count", nbins=10,
                               color_discrete_sequence=["#3a7bd5"])
            fig.update_layout(**PLOTLY_THEME, height=350,
                              xaxis_title="Items per Order", yaxis_title="Order Count")
            st.plotly_chart(fig, use_container_width=True)

    # AOV Trend
    st.markdown('<div class="section-header">💰 Average Order Value Trend</div>', unsafe_allow_html=True)
    aov = run_query("""
        SELECT d.year, d.month,
               d.month_name || ' ' || d.year as period,
               ROUND(SUM(f.net_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) as aov
        FROM fact_order_items f
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE f.order_status NOT IN ('cancelled','returned')
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """, engine)
    if not aov.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=aov["period"], y=aov["aov"],
                                 mode="lines+markers",
                                 line=dict(color="#00d2ff", width=3),
                                 fill="tozeroy", fillcolor="rgba(0,210,255,0.1)"))
        fig.update_layout(**PLOTLY_THEME, height=300, yaxis_title="AOV (₹)")
        st.plotly_chart(fig, use_container_width=True)

    if not status.empty and cancelled > 0:
        cancel_value = status[status["order_status"] == "cancelled"]["value"].sum()
        insight_box("Reducing Cancellations",
                    f"<b>{int(cancelled):,}</b> orders worth <b>{format_inr(cancel_value)}</b> were cancelled. "
                    f"Analyze the top cancellation reasons — common fixes include faster shipping confirmation, "
                    f"better product images, and estimated delivery dates at checkout.")


# ══════════════════════════════════════════════
# PAGE 6: PRODUCT PERFORMANCE
# ══════════════════════════════════════════════
def page_product_performance():
    st.markdown("## 📦 Product Performance")
    engine = get_engine()

    # Top Products
    top_products = run_query("""
        SELECT p.product_name, p.category, p.brand,
               SUM(f.quantity) as units_sold,
               SUM(f.net_amount) as revenue,
               SUM(f.gross_profit) as profit,
               ROUND(SUM(f.gross_profit) * 100.0 / NULLIF(SUM(f.net_amount), 0), 1) as margin_pct
        FROM fact_order_items f
        JOIN dim_product p ON f.product_key = p.product_key
        WHERE f.order_status NOT IN ('cancelled','returned')
        GROUP BY p.product_name, p.category, p.brand
        ORDER BY revenue DESC LIMIT 15
    """, engine)

    st.markdown('<div class="section-header">🏆 Top 15 Products by Revenue</div>', unsafe_allow_html=True)
    if not top_products.empty:
        fig = px.bar(top_products, x="revenue", y="product_name", orientation="h",
                     color="category", color_discrete_sequence=COLORS)
        fig.update_layout(**PLOTLY_THEME, height=450, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="section-header">📊 Category Revenue vs Units</div>', unsafe_allow_html=True)
        cat_perf = run_query("""
            SELECT p.category,
                   SUM(f.quantity) as units_sold,
                   SUM(f.net_amount) as revenue,
                   SUM(f.gross_profit) as profit,
                   COUNT(DISTINCT p.product_key) as product_count
            FROM fact_order_items f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY p.category ORDER BY revenue DESC
        """, engine)
        if not cat_perf.empty:
            fig = px.scatter(cat_perf, x="units_sold", y="revenue",
                             size="product_count", color="category",
                             hover_data=["profit"], color_discrete_sequence=COLORS,
                             size_max=50)
            fig.update_layout(**PLOTLY_THEME, height=350,
                              xaxis_title="Units Sold", yaxis_title="Revenue (₹)")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">📈 Brand Performance (Top 10)</div>', unsafe_allow_html=True)
        brands = run_query("""
            SELECT p.brand,
                   SUM(f.net_amount) as revenue,
                   SUM(f.quantity) as units
            FROM fact_order_items f
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY p.brand ORDER BY revenue DESC LIMIT 10
        """, engine)
        if not brands.empty:
            fig = px.bar(brands, x="brand", y="revenue",
                         color="revenue", color_continuous_scale="Blues")
            fig.update_layout(**PLOTLY_THEME, height=350, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

    # Low stock alert
    st.markdown('<div class="section-header">⚠️ Low Stock Products</div>', unsafe_allow_html=True)
    low_stock = run_query("""
        SELECT product_name, category, brand, stock_quantity, price
        FROM dim_product
        WHERE stock_quantity < 50 AND is_active = true
        ORDER BY stock_quantity ASC LIMIT 10
    """, engine)
    if not low_stock.empty:
        st.dataframe(low_stock.style.format({"price": "₹{:,.0f}"}),
                     use_container_width=True)
    else:
        st.info("✅ No products are critically low on stock.")


# ══════════════════════════════════════════════
# PAGE 7: PAYMENT ANALYTICS
# ══════════════════════════════════════════════
def page_payment_analytics():
    st.markdown("## 💳 Payment Analytics")
    engine = get_engine()

    payment_data = run_query("""
        SELECT pm.display_name as method,
               pm.method_type,
               COUNT(DISTINCT f.order_id) as transactions,
               SUM(f.net_amount) as revenue,
               ROUND(AVG(f.net_amount), 2) as avg_value
        FROM fact_order_items f
        JOIN dim_payment_method pm ON f.payment_method_key = pm.payment_method_key
        WHERE f.order_status NOT IN ('cancelled','returned')
        GROUP BY pm.display_name, pm.method_type
        ORDER BY revenue DESC
    """, engine)

    if not payment_data.empty:
        cols = st.columns(3)
        top = payment_data.iloc[0]
        with cols[0]:
            kpi_card("Most Popular Method", top["method"])
        total_txn = payment_data["transactions"].sum()
        digital = payment_data[payment_data["method_type"] == "digital"]["transactions"].sum()
        with cols[1]:
            kpi_card("Digital Payment Share", f"{digital/total_txn*100:.1f}%")
        with cols[2]:
            kpi_card("Total Transactions", f"{total_txn:,}")

        st.markdown("---")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">📊 Transaction Share by Method</div>', unsafe_allow_html=True)
            fig = px.pie(payment_data, names="method", values="transactions",
                         color_discrete_sequence=COLORS, hole=0.4)
            fig.update_layout(**PLOTLY_THEME, height=350)
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">💰 Revenue by Payment Method</div>', unsafe_allow_html=True)
            fig = px.bar(payment_data, x="method", y="revenue",
                         color="method_type", color_discrete_sequence=COLORS)
            fig.update_layout(**PLOTLY_THEME, height=350, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

        # Payment success rate
        st.markdown('<div class="section-header">✅ Payment Status by Method</div>', unsafe_allow_html=True)
        pay_status = run_query("""
            SELECT pm.display_name as method,
                   f.payment_status,
                   COUNT(DISTINCT f.order_id) as count
            FROM fact_order_items f
            JOIN dim_payment_method pm ON f.payment_method_key = pm.payment_method_key
            GROUP BY pm.display_name, f.payment_status
        """, engine)
        if not pay_status.empty:
            fig = px.bar(pay_status, x="method", y="count", color="payment_status",
                         barmode="group", color_discrete_sequence=COLORS)
            fig.update_layout(**PLOTLY_THEME, height=350)
            st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════
# PAGE 8: ACTIONABLE INSIGHTS
# ══════════════════════════════════════════════
def page_actionable_insights():
    st.markdown("## 🧠 Actionable Insights & Recommendations")
    engine = get_engine()

    st.markdown("""
    <div style="background: linear-gradient(135deg, #0f2027 0%, #203a43 100%);
                border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem;
                border: 1px solid rgba(99,179,237,0.3);">
        <h3 style="color: #63b3ed; margin-top: 0;">Data-Driven Action Plan</h3>
        <p style="color: #a0aec0;">
            Below are specific, data-backed recommendations derived from your warehouse analytics.
            Each insight includes the current metric, the opportunity, and a concrete next step.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ── Insight 1: Revenue Growth ──
    st.markdown("### 📈 1. How to Increase Revenue")
    rev_by_cat = run_query("""
        SELECT p.category,
               SUM(f.net_amount) as revenue,
               SUM(f.gross_profit) as profit,
               ROUND(SUM(f.gross_profit) * 100.0 / NULLIF(SUM(f.net_amount), 0), 1) as margin_pct,
               COUNT(DISTINCT f.order_id) as orders,
               SUM(f.quantity) as units
        FROM fact_order_items f
        JOIN dim_product p ON f.product_key = p.product_key
        WHERE f.order_status NOT IN ('cancelled','returned')
        GROUP BY p.category ORDER BY revenue DESC
    """, engine)

    if not rev_by_cat.empty:
        high_margin = rev_by_cat.nlargest(3, "margin_pct")
        low_rev_high_margin = rev_by_cat[
            (rev_by_cat["margin_pct"] > rev_by_cat["margin_pct"].median()) &
            (rev_by_cat["revenue"] < rev_by_cat["revenue"].median())
        ]

        insight_box("Promote High-Margin Categories",
                    f"Your highest-margin categories are: "
                    f"<b>{', '.join(high_margin['category'].tolist())}</b> "
                    f"({', '.join(high_margin['margin_pct'].astype(str).tolist())}% margins). "
                    f"Increase homepage visibility and ad spend for these categories.")

        if not low_rev_high_margin.empty:
            insight_box("Underperforming High-Margin Opportunity",
                        f"<b>{', '.join(low_rev_high_margin['category'].tolist())}</b> "
                        f"{'have' if len(low_rev_high_margin) > 1 else 'has'} above-average margins "
                        f"but below-average revenue. Bundle these with popular items to increase exposure.")

    # ── Insight 2: Convert New → Repeat ──
    st.markdown("### 🔄 2. How to Convert New Customers into Repeat Buyers")
    new_vs_repeat = run_query("""
        WITH cust_orders AS (
            SELECT customer_key, COUNT(DISTINCT order_id) as orders,
                   SUM(net_amount) as total_spent
            FROM fact_order_items
            WHERE order_status NOT IN ('cancelled','returned')
            GROUP BY customer_key
        )
        SELECT
            CASE WHEN orders = 1 THEN 'One-time' ELSE 'Repeat' END as customer_type,
            COUNT(*) as count,
            ROUND(AVG(total_spent), 2) as avg_spend
        FROM cust_orders
        GROUP BY CASE WHEN orders = 1 THEN 'One-time' ELSE 'Repeat' END
    """, engine)

    if not new_vs_repeat.empty:
        one_time = new_vs_repeat[new_vs_repeat["customer_type"] == "One-time"]
        repeat = new_vs_repeat[new_vs_repeat["customer_type"] == "Repeat"]

        if not one_time.empty and not repeat.empty:
            ot_count = int(one_time.iloc[0]["count"])
            rp_count = int(repeat.iloc[0]["count"])
            rp_spend = repeat.iloc[0]["avg_spend"]
            potential_rev = ot_count * 0.1 * rp_spend

            insight_box("The Repeat Customer Opportunity",
                        f"You have <b>{ot_count:,}</b> one-time buyers vs <b>{rp_count:,}</b> repeat buyers. "
                        f"Repeat buyers spend <b>{format_inr(rp_spend)}</b> on average. "
                        f"If just 10% of one-time buyers return, that's <b>{format_inr(potential_rev)}</b> "
                        f"in additional revenue.")

            insight_box("Action: Post-Purchase Email Sequence",
                        "Send a 3-part email sequence: "
                        "<b>Day 3</b> – Delivery feedback request. "
                        "<b>Day 7</b> – Personalized 'You might also like' recommendations. "
                        "<b>Day 14</b> – ₹100 off coupon with 7-day expiry to create urgency.")

    # ── Insight 3: Premium Upgrade Path ──
    st.markdown("### 🌟 3. How to Upgrade Regular to Premium Customers")
    regular_close = run_query("""
        WITH cust_stats AS (
            SELECT c.customer_key, c.customer_id,
                   c.first_name || ' ' || c.last_name as name,
                   c.segment, c.city,
                   COUNT(DISTINCT f.order_id) as orders,
                   SUM(f.net_amount) as total_spent
            FROM fact_order_items f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            WHERE f.order_status NOT IN ('cancelled','returned')
            GROUP BY c.customer_key, c.customer_id, c.first_name, c.last_name, c.segment, c.city
        )
        SELECT * FROM cust_stats
        WHERE segment = 'Regular'
          AND (total_spent > 7000 OR orders >= 4)
        ORDER BY total_spent DESC LIMIT 20
    """, engine)

    if not regular_close.empty:
        insight_box("Near-Premium Regular Customers",
                    f"<b>{len(regular_close)}</b> Regular customers are close to Premium thresholds "
                    f"(₹10,000 spend or 5+ orders). These are your highest-potential upgrade candidates.")

        st.dataframe(
            regular_close[["name", "city", "orders", "total_spent"]].style.format({"total_spent": "₹{:,.0f}"}),
            use_container_width=True, height=300
        )

        insight_box("Action: VIP Preview Program",
                    "Offer these customers early access to sales and exclusive discounts. "
                    "Frame it as 'You're one order away from Premium status — unlock free shipping, "
                    "priority support, and 5% extra rewards on every purchase.'")

    # ── Insight 4: Geographic Expansion ──
    st.markdown("### 🗺 4. Geographic Growth Opportunities")
    geo_analysis = run_query("""
        SELECT l.region, l.tier,
               COUNT(DISTINCT f.customer_key) as customers,
               SUM(f.net_amount) as revenue,
               ROUND(SUM(f.net_amount) / NULLIF(COUNT(DISTINCT f.customer_key), 0), 2) as rev_per_customer
        FROM fact_order_items f
        JOIN dim_location l ON f.location_key = l.location_key
        WHERE f.order_status NOT IN ('cancelled','returned')
        GROUP BY l.region, l.tier
        ORDER BY revenue DESC
    """, engine)

    if not geo_analysis.empty:
        fig = px.treemap(geo_analysis, path=["region", "tier"],
                         values="revenue", color="rev_per_customer",
                         color_continuous_scale="Blues",
                         title="Revenue by Region and City Tier")
        fig.update_layout(**PLOTLY_THEME, height=400)
        st.plotly_chart(fig, use_container_width=True)

        # Find underperforming regions
        avg_rpc = geo_analysis["rev_per_customer"].mean()
        low_rpc = geo_analysis[geo_analysis["rev_per_customer"] < avg_rpc * 0.8]
        if not low_rpc.empty:
            regions = ", ".join(f"{r['region']} {r['tier']}" for _, r in low_rpc.iterrows())
            insight_box("Underperforming Regions",
                        f"The following regions have below-average revenue per customer: <b>{regions}</b>. "
                        f"Consider localized marketing campaigns and regional festive offers.")


# ══════════════════════════════════════════════
# SIDEBAR & ROUTING
# ══════════════════════════════════════════════
PAGES = {
    "📊 Executive Summary": page_executive_summary,
    "📈 Revenue Analytics": page_revenue_analytics,
    "👥 Customer Intelligence": page_customer_intelligence,
    "🔄 Retention & Churn": page_retention_churn,
    "🛒 Conversion & Orders": page_conversion,
    "📦 Product Performance": page_product_performance,
    "💳 Payment Analytics": page_payment_analytics,
    "🧠 Actionable Insights": page_actionable_insights,
}

# Sidebar
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0;">
        <h2 style="color: white; margin: 0;">🛍️ E-Commerce</h2>
        <p style="color: #a0aec0; margin: 0; font-size: 0.9rem;">Business Intelligence</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    page = st.radio("Navigation", list(PAGES.keys()), label_visibility="collapsed")

    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #4a5568; font-size: 0.75rem; padding: 1rem 0;">
        Data Pipeline v2.0<br>
        Star Schema Architecture<br>
        Last Refresh: Auto (5 min cache)
    </div>
    """, unsafe_allow_html=True)

# Render selected page
PAGES[page]()
