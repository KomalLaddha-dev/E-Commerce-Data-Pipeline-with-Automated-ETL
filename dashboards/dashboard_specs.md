# Dashboard Specifications
## E-commerce Data Pipeline with Automated ETL

This document defines the layout, KPIs, and data sources for each dashboard
that connects to the `ecommerce_dw` data warehouse.

---

## 1. Sales Dashboard

### Purpose
Provides a real-time overview of revenue, orders, and sales trends for
executive decision-making.

### Data Source
- Primary: `sales_fact` joined with `date_dim`
- Materialized view: `mv_daily_sales_summary`

### KPI Cards (Top Row)

| KPI | SQL Metric | Format |
|-----|-----------|--------|
| Total Revenue | `SUM(net_amount)` | Currency (INR) |
| Total Orders | `COUNT(DISTINCT order_id)` | Number |
| Avg Order Value | `AVG(net_amount)` | Currency (INR) |
| MoM Growth | Month-over-month % change | Percentage |

### Charts

| Chart | Type | X-Axis | Y-Axis | Filter |
|-------|------|--------|--------|--------|
| Revenue Trend | Line | Date (daily/weekly/monthly) | Net Revenue | Date range picker |
| Sales by Category | Pie / Donut | Category | Revenue share % | Current period |
| Top 10 Products | Horizontal Bar | Product name | Revenue | Date range |
| Orders by Status | Stacked Bar | Status | Order count | Date range |
| Hourly Sales Heatmap | Heatmap | Hour of day | Day of week | Last 30 days |

### Filters
- Date range (default: last 30 days)
- Product category
- Order status (exclude cancelled/returned by default)

### Refresh Schedule
- Auto-refresh every 15 minutes during business hours
- Full refresh after daily ETL at 02:30 UTC

---

## 2. Customer Analytics Dashboard

### Purpose
Tracks customer acquisition, retention, segmentation, and lifetime value
to support marketing and CRM strategy.

### Data Source
- `sales_fact` joined with `customer_dim` and `date_dim`

### KPI Cards (Top Row)

| KPI | SQL Metric | Format |
|-----|-----------|--------|
| Total Customers | `COUNT(DISTINCT customer_id)` | Number |
| New This Month | Customers with `registration_date` in current month | Number |
| Avg CLV | `AVG(lifetime_value)` from CLV query | Currency (INR) |
| Retention Rate | Returning customers / Total customers * 100 | Percentage |

### Charts

| Chart | Type | Description |
|-------|------|-------------|
| Customer Segments | Donut | Premium / Regular / New breakdown |
| CLV Distribution | Histogram | Distribution of customer lifetime values |
| Acquisition Trend | Area | New customers per month |
| Top Customers | Table | Top 20 by lifetime value with order count |
| Geographic Heatmap | Map | Revenue by state/city |
| Repeat vs One-Time | Bar | Customer type breakdown |

### Filters
- Date range
- Customer segment (Premium / Regular / New)
- City / State

---

## 3. Product Performance Dashboard

### Purpose
Monitors product sales velocity, inventory health, margin performance,
and category-level trends for merchandising decisions.

### Data Source
- `sales_fact` joined with `product_dim`
- Materialized view: `mv_product_performance`

### KPI Cards (Top Row)

| KPI | SQL Metric | Format |
|-----|-----------|--------|
| Active Products | `COUNT(*)` where `is_active = true` | Number |
| Avg Profit Margin | `AVG(profit_margin)` | Percentage |
| Low Stock Items | Products with `stock_quantity <= 50` | Number |
| Avg Selling Price | `AVG(unit_price)` from fact | Currency (INR) |

### Charts

| Chart | Type | Description |
|-------|------|-------------|
| Revenue by Category | Treemap | Nested category / sub-category revenue |
| Top Sellers | Horizontal Bar | Top 10 products by units sold |
| Margin Analysis | Scatter | Price vs margin with bubble = units sold |
| Inventory Status | Table | Products sorted by stock level with RAG status |
| Category Trend | Multi-line | Monthly revenue per category |
| Brand Performance | Bar | Revenue by brand (top 10) |

### Filters
- Product category
- Brand
- Stock status (Critical / Low / Medium / Healthy)
- Date range

---

## 4. Payment Analytics Dashboard

### Purpose
Analyzes payment method preferences, transaction success rates,
and revenue by payment channel.

### Data Source
- `sales_fact` joined with `payment_dim`

### KPI Cards (Top Row)

| KPI | SQL Metric | Format |
|-----|-----------|--------|
| Total Transactions | `COUNT(*)` | Number |
| Completion Rate | `completed / total * 100` | Percentage |
| Most Popular Method | Mode of `payment_method` | Text |
| Avg Transaction Value | `AVG(net_amount)` | Currency (INR) |

### Charts

| Chart | Type | Description |
|-------|------|-------------|
| Payment Method Share | Pie | Revenue % by payment method |
| Method Trend | Stacked Area | Monthly payment method usage |
| Success Rate by Method | Bar | Completion rate per method |
| Avg Value by Method | Bar | Average transaction size per method |

---

## Power BI Connection Guide

### Step 1: Connect to PostgreSQL
```
1. Open Power BI Desktop
2. Home → Get Data → PostgreSQL Database
3. Server: localhost (or warehouse host)
4. Port: 5432
5. Database: ecommerce_dw
6. Data Connectivity Mode: DirectQuery (recommended for live data)
                           OR Import (for offline analysis)
```

### Step 2: Select Tables
```
Select all star schema tables:
  ☑ sales_fact
  ☑ customer_dim
  ☑ product_dim
  ☑ date_dim
  ☑ payment_dim
  ☑ mv_daily_sales_summary (materialized view)
  ☑ mv_product_performance (materialized view)
```

### Step 3: Define Relationships
```
sales_fact.customer_key  →  customer_dim.customer_key  (Many-to-One)
sales_fact.product_key   →  product_dim.product_key    (Many-to-One)
sales_fact.date_key      →  date_dim.date_key          (Many-to-One)
sales_fact.payment_key   →  payment_dim.payment_key    (Many-to-One)
```

### Step 4: Create Measures (DAX)
```dax
// Total Revenue
Total Revenue = SUM(sales_fact[net_amount])

// Total Orders
Total Orders = DISTINCTCOUNT(sales_fact[order_id])

// Average Order Value
Avg Order Value = DIVIDE([Total Revenue], [Total Orders], 0)

// Month-over-Month Growth
MoM Growth =
VAR CurrentMonth = [Total Revenue]
VAR PrevMonth = CALCULATE([Total Revenue], DATEADD(date_dim[full_date], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PrevMonth, PrevMonth, 0)

// Customer Lifetime Value
CLV = DIVIDE(SUM(sales_fact[net_amount]), DISTINCTCOUNT(sales_fact[customer_key]), 0)
```

---

## Tableau Connection Guide

### Step 1: Connect to PostgreSQL
```
1. Open Tableau Desktop
2. Connect → To a Server → PostgreSQL
3. Server: localhost
4. Port: 5432
5. Database: ecommerce_dw
6. Authentication: Username and Password
```

### Step 2: Drag Tables
```
Drag sales_fact to canvas as the central table.
Join dimension tables:
  sales_fact ← Inner Join → customer_dim  (ON customer_key)
  sales_fact ← Inner Join → product_dim   (ON product_key)
  sales_fact ← Inner Join → date_dim      (ON date_key)
  sales_fact ← Inner Join → payment_dim   (ON payment_key)
```

### Step 3: Create Calculated Fields
```
// Net Revenue
SUM([Net Amount])

// Profit Margin %
([Price] - [Cost Price]) / [Price] * 100

// Customer Segment Color
IF [Segment] = "Premium" THEN "Green"
ELSEIF [Segment] = "Regular" THEN "Blue"
ELSE "Orange"
END
```

---

## Color Scheme

| Element | Hex Code | Usage |
|---------|---------|-------|
| Primary Blue | #2563EB | Headers, KPI cards |
| Success Green | #16A34A | Positive trends, healthy stock |
| Warning Amber | #D97706 | Low stock, declining metrics |
| Danger Red | #DC2626 | Critical stock, failed payments |
| Light Gray | #F3F4F6 | Card backgrounds |
| Dark Text | #1F2937 | Primary text |
| Muted Text | #6B7280 | Secondary labels |
