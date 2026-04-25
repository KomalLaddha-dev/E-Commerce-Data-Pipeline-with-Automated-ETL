# E-Commerce Data Pipeline with Automated ETL
## Project Presentation

---

## Slide 1: Title

### 🛍️ E-Commerce Data Pipeline
#### Automated ETL with Star Schema Analytics

**Built with:** Python • PostgreSQL • Apache Airflow • Streamlit

**Data Scale:** 2,000 Customers | 500 Products | 15,000 Orders | 30,000+ Line Items

---

## Slide 2: Problem Statement

### The Challenge

E-commerce businesses generate massive volumes of transactional data across customers, products, orders, and payments. Without a proper data infrastructure:

- ❌ **No centralized analytics** — data scattered across operational tables
- ❌ **Slow ad-hoc queries** — OLTP databases not optimized for analytics
- ❌ **No actionable insights** — raw data without business context
- ❌ **Manual data processing** — error-prone, inconsistent results
- ❌ **No customer intelligence** — can't segment, can't predict churn

### The Need

A production-grade **automated data pipeline** that transforms raw transactional data into a **decision-ready analytics platform** with real-time business intelligence.

---

## Slide 3: Solution Overview

### End-to-End Data Platform

```
Source OLTP ──► Extract ──► Transform ──► Load ──► Star Schema DW ──► Dashboard
(PostgreSQL)   (Raw CSV)   (Cleaned)    (DW)     (5 Dims + 1 Fact)   (8 Pages)
```

**Key Deliverables:**
1. ✅ Automated ETL pipeline with scheduling
2. ✅ Star schema data warehouse (Kimball methodology)
3. ✅ 8-page business intelligence dashboard
4. ✅ Actionable insights engine with recommendations
5. ✅ Docker-based deployment (one command)
6. ✅ Data quality validation pipeline

---

## Slide 4: Architecture

### System Architecture

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Source** | PostgreSQL (OLTP) | Transactional data storage |
| **Extraction** | Python + SQLAlchemy | Pull data with watermark tracking |
| **Transformation** | pandas + numpy | Clean, enrich, build star schema |
| **Loading** | SQLAlchemy bulk ops | Dimension reload + fact truncate-append |
| **Warehouse** | PostgreSQL (OLAP) | Star schema with materialized views |
| **Orchestration** | Apache Airflow | Daily scheduling with retry logic |
| **Visualization** | Streamlit + Plotly | Interactive 8-page dashboard |
| **Infrastructure** | Docker Compose | 3-service deployment |

---

## Slide 5: Star Schema Design

### Data Warehouse Model

**Fact Table (Grain: One row per order line item)**
- `fact_order_items` — quantity, unit_price, net_amount, gross_profit

**Dimension Tables (5)**
- `dim_date` — calendar hierarchy (day → month → quarter → year)
- `dim_customer` — profiles + segments (Premium / Regular / New)
- `dim_product` — catalog with margins (8 categories, 80+ products)
- `dim_payment_method` — conformed dimension (6 methods)
- `dim_location` — geographic hierarchy (city → state → region → tier)

**Materialized Views (3)**
- `mv_daily_sales` — pre-aggregated daily revenue
- `mv_product_performance` — product-level KPIs
- `mv_customer_summary` — customer lifetime metrics

---

## Slide 6: Data Quality & Realism

### Realistic Indian E-Commerce Dataset

| Feature | Before (v1) | After (v2) |
|---------|-------------|------------|
| **Customer Names** | SyntheticFirst123 / SyntheticLast456 | 130+ real Indian names (Aarav Sharma, Priya Patel...) |
| **City Coverage** | 12 cities, random uniform | 40 cities, population-weighted (Mumbai 14%, Delhi 12%...) |
| **Products** | Synthetic Product XYZ-123 | 82 real products with brands (boAt, Samsung, Nike, Levi's...) |
| **Payment Methods** | Random uniform | India market: UPI 42%, Credit 18%, COD 7% |
| **Order Patterns** | Flat distribution | Seasonal spikes (Diwali, year-end), hourly patterns |
| **Geographic Detail** | City only | City + State + Region + Tier (Tier 1/2/3) |

---

## Slide 7: ETL Pipeline

### Automated Extract → Transform → Load

**Extract Phase:**
- Pulls from 5 source tables with incremental watermark tracking
- Outputs timestamped CSV files to staging area

**Transform Phase:**
- Data cleaning: deduplication, type casting, null handling
- Status normalization (e.g., "Delvrd" → "delivered")
- Customer segment enrichment (RFM-based: Premium/Regular/New)
- Profit margin calculation per product
- Surrogate key generation for all dimensions
- Pro-rata allocation of order-level discounts to line items

**Load Phase:**
- Dimensions: full reload (small tables, ~2K rows max)
- Facts: truncate-and-append in 5K chunks
- Materialized view refresh (CONCURRENTLY)
- ETL audit logging with batch IDs

---

## Slide 8: Dashboard — Executive Summary

### Page 1: Executive Summary

**KPI Cards:**
- Total Revenue | Gross Profit | Total Orders | Unique Customers | Avg Line Value
- MoM (Month-over-Month) growth indicators

**Charts:**
- Monthly Revenue Trend (bar) with Profit overlay (line)
- Category Revenue Split (donut chart)
- Customer Segment Distribution (grouped bar)
- Top 10 Cities by Revenue (horizontal bar)

---

## Slide 9: Dashboard — Revenue & Customer Intelligence

### Page 2: Revenue Analytics
- Selectable granularity: Daily / Weekly / Monthly / Quarterly
- Weekend vs Weekday comparison
- Revenue by Day of Week
- Profit Margin by Category with insight callout

### Page 3: Customer Intelligence
- Segment KPI cards (Premium 🌟, Regular 👤, New 🆕)
- Revenue by Segment (pie chart)
- Customer Lifetime Value histogram by segment
- Top 15 Customers table with LTV, tenure, city

---

## Slide 10: Dashboard — Retention & Conversion

### Page 4: Retention & Churn
- Purchase Frequency Distribution (One-time / Returning / Loyal / VIP)
- Repeat Purchase Rate KPI
- Monthly Active Customers (area chart)
- **Cohort Retention Heatmap** — tracks customer return rates by month

### Page 5: Conversion & Orders
- Order Status Funnel (delivered → shipped → pending → cancelled → returned)
- Items per Order Distribution
- Average Order Value (AOV) Trend
- Cancellation and Return Rate KPIs

---

## Slide 11: Dashboard — Products & Payments

### Page 6: Product Performance
- Top 15 Products by Revenue (horizontal bar, colored by category)
- Category Revenue vs Units scatter plot (bubble size = product count)
- Brand Performance Rankings (Top 10)
- **Low Stock Alert** table — products below 50 units

### Page 7: Payment Analytics
- Transaction Share by Payment Method (donut)
- Revenue by Payment Method (bar, colored by type: digital/card/cash)
- Digital Payment Share KPI
- Payment Status by Method (grouped bar: completed/pending/failed/refunded)

---

## Slide 12: Dashboard — Actionable Insights

### Page 8: 🧠 Actionable Insights Engine

Unlike traditional dashboards that just visualize data, this page provides **prescriptive recommendations**:

**1. How to Increase Revenue**
> Identifies high-margin, low-revenue categories → recommends increased visibility

**2. How to Convert New → Repeat Buyers**
> Calculates the revenue impact of a 10% one-time → repeat conversion
> Prescribes a 3-part email sequence (Day 3, Day 7, Day 14)

**3. How to Upgrade Regular → Premium**
> Lists specific customers near Premium thresholds
> Recommends a VIP Preview Program with early access incentives

**4. Geographic Growth Opportunities**
> Treemap of revenue by Region × Tier
> Highlights underperforming regions with below-average revenue per customer

---

## Slide 13: Data Quality Pipeline

### Automated Quality Gates

The ETL pipeline includes a **mandatory quality check step** between loading and view refresh:

| Check | Validation |
|-------|-----------|
| ✅ Row Count | fact_order_items is not empty |
| ✅ Null Keys | No null customer_key, date_key, product_key |
| ✅ Negative Amounts | No negative net_amount values |
| ✅ Referential Integrity | All date_keys exist in dim_date |
| ✅ Dimension Completeness | All 5 dimension tables are non-empty |

If any check fails → pipeline halts + email notification via Airflow.

---

## Slide 14: Deployment & Infrastructure

### One-Command Deployment

```bash
docker compose up --build
```

This spins up **3 services:**
1. **PostgreSQL 15** — Source DB + Warehouse DB
2. **ETL Pipeline** — Setup → Generate Data → Extract → Transform → Load
3. **Streamlit Dashboard** — Auto-connected to warehouse at port 8501

**Resource Requirements:**
- Docker Desktop + Docker Compose v2
- 4 GB RAM minimum
- ~2 GB disk for databases + images

---

## Slide 15: Business Impact & Future Scope

### Business Impact

| Metric | Impact |
|--------|--------|
| **Decision Speed** | From manual SQL queries to real-time dashboard |
| **Data Freshness** | Daily automated refresh (configurable to hourly) |
| **Actionable Insights** | Prescriptive recommendations, not just descriptive charts |
| **Segment Intelligence** | Identify and convert high-value customer segments |
| **Revenue Optimization** | Margin analysis reveals promotion opportunities |

### Future Scope

- 🔮 **Predictive Analytics** — ML-based demand forecasting and churn prediction
- 📱 **Real-time Streaming** — Kafka/Debezium CDC for near real-time updates
- ☁️ **Cloud Deployment** — AWS RDS + ECS/Fargate migration
- 🤖 **AI Chatbot** — Natural language query interface for business users
- 📊 **A/B Testing** — Integrated experiment tracking for pricing strategies
- 🏷️ **Recommendation Engine** — Collaborative filtering for product recommendations

---

## Thank You

**Repository:** [GitHub - E-Commerce-Data-Pipeline-with-Automated-ETL](https://github.com/KomalLaddha-dev/E-Commerce-Data-Pipeline-with-Automated-ETL)

**Tech Stack:** Python 3.11 • PostgreSQL 15 • Apache Airflow • Streamlit • Plotly • Docker
