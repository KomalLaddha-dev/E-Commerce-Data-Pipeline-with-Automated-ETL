"""
app.py — Interactive E-commerce Analytics Dashboard
E-commerce Data Pipeline with Automated ETL

Connects to the PostgreSQL data warehouse and displays
real-time KPIs, charts, and tables using Streamlit.

Run: streamlit run dashboards/app.py
Docker: Automatically available at http://localhost:8501
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
DW_HOST = os.environ.get("DW_HOST", "localhost")
DW_PORT = os.environ.get("DW_PORT", "5433")
DW_USER = os.environ.get("DW_USER", "etl_user")
DW_PASSWORD = os.environ.get("DW_PASSWORD", "etl_password")
DW_NAME = os.environ.get("DW_NAME", "ecommerce_dw")
DW_DRIVER = os.environ.get("DW_DRIVER", "postgresql")

CONN_STRING = f"{DW_DRIVER}://{DW_USER}:{DW_PASSWORD}@{DW_HOST}:{DW_PORT}/{DW_NAME}"

# ──────────────────────────────────────────────
# Page Config
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="E-commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_engine():
    """Create a cached database engine."""
    return create_engine(CONN_STRING, pool_pre_ping=True)


@st.cache_data(ttl=60)
def run_query(query):
    """Run a SQL query and return a DataFrame. Results cached for 60s."""
    engine = get_engine()
    return pd.read_sql(text(query), engine)


# ──────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────
st.sidebar.title("📊 E-commerce Analytics")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["🏠 Sales Overview", "👥 Customer Analytics", "📦 Product Performance", "💳 Payment Analysis"],
)

st.sidebar.markdown("---")
st.sidebar.caption("E-commerce Data Pipeline with Automated ETL")
st.sidebar.caption("Powered by PostgreSQL + Streamlit")


# ══════════════════════════════════════════════
# PAGE 1: Sales Overview
# ══════════════════════════════════════════════
if page == "🏠 Sales Overview":
    st.title("🏠 Sales Overview")
    st.markdown("Real-time revenue, orders, and sales trends from the data warehouse.")

    # ── KPI Cards ──
    kpi_data = run_query("""
        SELECT
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(net_amount) AS total_revenue,
            ROUND(AVG(net_amount), 2) AS avg_order_value,
            SUM(total_items) AS total_items_sold
        FROM sales_fact
        WHERE order_status NOT IN ('cancelled', 'returned')
    """)

    if not kpi_data.empty:
        row = kpi_data.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Orders", f"{int(row['total_orders']):,}")
        col2.metric("Total Revenue", f"₹{row['total_revenue']:,.2f}")
        col3.metric("Avg Order Value", f"₹{row['avg_order_value']:,.2f}")
        col4.metric("Items Sold", f"{int(row['total_items_sold']):,}")

    st.markdown("---")

    # ── Daily Revenue Chart ──
    daily = run_query("""
        SELECT
            d.full_date,
            d.day_name,
            COUNT(DISTINCT f.order_id) AS orders,
            SUM(f.net_amount) AS revenue
        FROM sales_fact f
        JOIN date_dim d ON f.date_key = d.date_key
        WHERE f.order_status NOT IN ('cancelled', 'returned')
        GROUP BY d.full_date, d.day_name
        ORDER BY d.full_date
    """)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📈 Daily Revenue Trend")
        if not daily.empty:
            fig = px.bar(
                daily, x="full_date", y="revenue",
                color_discrete_sequence=["#2563EB"],
                labels={"full_date": "Date", "revenue": "Revenue (₹)"},
            )
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sales data available.")

    # ── Order Status Distribution ──
    status = run_query("""
        SELECT order_status, COUNT(*) AS count, SUM(total_amount) AS value
        FROM sales_fact GROUP BY order_status ORDER BY count DESC
    """)

    with col_right:
        st.subheader("📋 Order Status Distribution")
        if not status.empty:
            colors = {
                "delivered": "#16A34A", "shipped": "#2563EB",
                "confirmed": "#7C3AED", "pending": "#D97706",
                "cancelled": "#DC2626", "returned": "#6B7280",
            }
            fig = px.pie(
                status, values="count", names="order_status",
                color="order_status",
                color_discrete_map=colors,
                hole=0.4,
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Daily Revenue Table ──
    st.subheader("📊 Daily Sales Detail")
    if not daily.empty:
        display_df = daily.copy()
        display_df.columns = ["Date", "Day", "Orders", "Revenue (₹)"]
        display_df["Revenue (₹)"] = display_df["Revenue (₹)"].apply(lambda x: f"₹{x:,.2f}")
        st.dataframe(display_df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════
# PAGE 2: Customer Analytics
# ══════════════════════════════════════════════
elif page == "👥 Customer Analytics":
    st.title("👥 Customer Analytics")
    st.markdown("Customer segmentation, lifetime value, and geographic distribution.")

    # ── KPIs ──
    cust_kpi = run_query("""
        SELECT
            COUNT(DISTINCT customer_id) AS total_customers,
            COUNT(DISTINCT CASE WHEN segment = 'Premium' THEN customer_id END) AS premium,
            COUNT(DISTINCT CASE WHEN segment = 'Regular' THEN customer_id END) AS regular,
            COUNT(DISTINCT CASE WHEN segment = 'New' THEN customer_id END) AS new_customers
        FROM customer_dim
    """)

    if not cust_kpi.empty:
        row = cust_kpi.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Customers", int(row["total_customers"]))
        c2.metric("Premium", int(row["premium"]), delta="⭐")
        c3.metric("Regular", int(row["regular"]))
        c4.metric("New", int(row["new_customers"]))

    st.markdown("---")
    col_left, col_right = st.columns(2)

    # ── Segment Breakdown ──
    with col_left:
        st.subheader("🎯 Customer Segments")
        segments = run_query("""
            SELECT segment, COUNT(*) AS count
            FROM customer_dim GROUP BY segment ORDER BY count DESC
        """)
        if not segments.empty:
            fig = px.pie(
                segments, values="count", names="segment",
                color="segment",
                color_discrete_map={"Premium": "#16A34A", "Regular": "#2563EB", "New": "#D97706"},
                hole=0.45,
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ── Customer Lifetime Value ──
    with col_right:
        st.subheader("💰 Top Customers by Revenue")
        clv = run_query("""
            SELECT
                c.first_name || ' ' || c.last_name AS customer,
                c.city,
                c.segment,
                COUNT(DISTINCT f.order_id) AS orders,
                SUM(f.net_amount) AS revenue
            FROM sales_fact f
            JOIN customer_dim c ON f.customer_id = c.customer_id
            WHERE f.order_status NOT IN ('cancelled', 'returned')
            GROUP BY c.first_name, c.last_name, c.city, c.segment
            ORDER BY revenue DESC
            LIMIT 10
        """)
        if not clv.empty:
            fig = px.bar(
                clv, x="revenue", y="customer", orientation="h",
                color="segment",
                color_discrete_map={"Premium": "#16A34A", "Regular": "#2563EB", "New": "#D97706"},
                labels={"revenue": "Revenue (₹)", "customer": ""},
            )
            fig.update_layout(height=350, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Geographic Distribution ──
    st.subheader("🌍 Customers by City")
    geo = run_query("""
        SELECT city, state, COUNT(*) AS customers
        FROM customer_dim GROUP BY city, state ORDER BY customers DESC
    """)
    if not geo.empty:
        fig = px.bar(
            geo, x="city", y="customers",
            color="state",
            labels={"city": "City", "customers": "Customer Count"},
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════
# PAGE 3: Product Performance
# ══════════════════════════════════════════════
elif page == "📦 Product Performance":
    st.title("📦 Product Performance")
    st.markdown("Product catalog analysis, margins, and category breakdown.")

    # ── KPIs ──
    prod_kpi = run_query("""
        SELECT
            COUNT(*) AS total_products,
            ROUND(AVG(profit_margin), 1) AS avg_margin,
            ROUND(AVG(price), 2) AS avg_price,
            COUNT(CASE WHEN stock_quantity < 50 THEN 1 END) AS low_stock
        FROM product_dim
    """)

    if not prod_kpi.empty:
        row = prod_kpi.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Products", int(row["total_products"]))
        c2.metric("Avg Margin", f"{row['avg_margin']}%")
        c3.metric("Avg Price", f"₹{row['avg_price']:,.2f}")
        c4.metric("Low Stock Items", int(row["low_stock"]))

    st.markdown("---")
    col_left, col_right = st.columns(2)

    # ── Category Revenue ──
    with col_left:
        st.subheader("📂 Products by Category")
        cat = run_query("""
            SELECT category, COUNT(*) AS products, ROUND(AVG(profit_margin), 1) AS avg_margin
            FROM product_dim GROUP BY category ORDER BY products DESC
        """)
        if not cat.empty:
            fig = px.bar(
                cat, x="category", y="products",
                color="avg_margin", color_continuous_scale="Greens",
                labels={"category": "Category", "products": "Product Count", "avg_margin": "Avg Margin %"},
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ── Price vs Margin Scatter ──
    with col_right:
        st.subheader("💎 Price vs Profit Margin")
        scatter_data = run_query("""
            SELECT product_name, price, profit_margin, category, stock_quantity
            FROM product_dim WHERE price > 0
        """)
        if not scatter_data.empty:
            fig = px.scatter(
                scatter_data, x="price", y="profit_margin",
                size="stock_quantity", color="category",
                hover_name="product_name",
                labels={"price": "Price (₹)", "profit_margin": "Margin %"},
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Product Table ──
    st.subheader("📋 Full Product Catalog")
    products = run_query("""
        SELECT product_name, category, brand, price, cost_price,
               profit_margin, stock_quantity
        FROM product_dim ORDER BY price DESC
    """)
    if not products.empty:
        products.columns = ["Product", "Category", "Brand", "Price (₹)", "Cost (₹)", "Margin %", "Stock"]
        st.dataframe(products, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════
# PAGE 4: Payment Analysis
# ══════════════════════════════════════════════
elif page == "💳 Payment Analysis":
    st.title("💳 Payment Analysis")
    st.markdown("Payment method preferences, transaction volumes, and success rates.")

    # ── KPIs ──
    pay_kpi = run_query("""
        SELECT
            COUNT(*) AS total_transactions,
            COUNT(CASE WHEN payment_status = 'completed' THEN 1 END) AS completed,
            ROUND(AVG(amount), 2) AS avg_amount,
            ROUND(
                COUNT(CASE WHEN payment_status = 'completed' THEN 1 END) * 100.0 / COUNT(*), 1
            ) AS success_rate
        FROM payment_dim
    """)

    if not pay_kpi.empty:
        row = pay_kpi.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Transactions", int(row["total_transactions"]))
        c2.metric("Completed", int(row["completed"]))
        c3.metric("Avg Transaction", f"₹{row['avg_amount']:,.2f}")
        c4.metric("Success Rate", f"{row['success_rate']}%")

    st.markdown("---")
    col_left, col_right = st.columns(2)

    # ── Payment Method Share ──
    with col_left:
        st.subheader("🥧 Revenue by Payment Method")
        method = run_query("""
            SELECT payment_method, COUNT(*) AS transactions, SUM(amount) AS total
            FROM payment_dim GROUP BY payment_method ORDER BY total DESC
        """)
        if not method.empty:
            fig = px.pie(
                method, values="total", names="payment_method",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ── Payment Status ──
    with col_right:
        st.subheader("📊 Transaction Status")
        status = run_query("""
            SELECT payment_status, COUNT(*) AS count, SUM(amount) AS total
            FROM payment_dim GROUP BY payment_status ORDER BY count DESC
        """)
        if not status.empty:
            fig = px.bar(
                status, x="payment_status", y="count",
                color="payment_status",
                color_discrete_map={
                    "completed": "#16A34A", "pending": "#D97706",
                    "failed": "#DC2626", "refunded": "#6B7280",
                },
                labels={"payment_status": "Status", "count": "Transactions"},
            )
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Payment Method Table ──
    st.subheader("📋 Payment Method Breakdown")
    if not method.empty:
        method_display = method.copy()
        method_display.columns = ["Method", "Transactions", "Total (₹)"]
        method_display["Total (₹)"] = method_display["Total (₹)"].apply(lambda x: f"₹{x:,.2f}")
        st.dataframe(method_display, use_container_width=True, hide_index=True)
