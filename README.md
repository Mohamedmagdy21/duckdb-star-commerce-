# Analytical SQL Project (E-Commerce Star Schema)

This repository contains an end-to-end analytical SQL project built on a **star-schema data warehouse** for an e-commerce domain. It uses **DuckDB + SQL window functions** to compute KPIs, time-based analytics, ranking/contribution metrics, customer behavior analytics, and advanced volatility/seasonality features. It also includes a recommendation-system notebook.

## What’s inside

### Data / Warehouse model
The project generates (and/or loads) a local DuckDB database `ecommerce.db` with:

- `Fact_Order_Line` (fact table; one row per order line item)
- `Dim_Date`
- `Dim_Customer`
- `Dim_Product`
- `Dim_Category`
- `Dim_Payment`
- `Dim_Shipping`

The notebooks perform analytics by joining `Fact_Order_Line` to the relevant dimensions.

### Analytical notebooks

| Notebook | Focus |
|---|---|
| `01_setup_and_data.ipynb` | Create/load `ecommerce.db`, define the schema, and populate sample data |
| `02_core_kpis.ipynb` | Core KPI calculations (SQL-driven) |
| `03a_time_based_analysis.ipynb` | Time-series analytics using window functions (cumulative, MTD, YTD, moving averages, MoM comparisons, etc.) |
| `03b_ranking_contribution.ipynb` | Ranking and contribution analysis (rank products, contribution ratios, Pareto/80-20) |
| `03c_customer_behavior.ipynb` | Customer behavioral analytics (cumulative spending, inter-purchase time, recency, tiers, percentile) |
| `03d_advanced_analytics.ipynb` | Advanced analytics (volatility, margin consistency, seasonality, trend detection) |
| `04_recommendation_system.ipynb` | Recommendation system using the generated dataset |

## Tech stack

- Python (Jupyter notebooks)
- DuckDB (SQL execution + window functions)
- Pandas / NumPy (data handling)
- Optional plotting + ML dependencies used by some notebooks

## How to run

1. Open `01_setup_and_data.ipynb`
2. Run the setup cells to create/load `ecommerce.db`
3. Then run the analysis notebooks in this order:
   - `02_core_kpis.ipynb`
   - `03a_time_based_analysis.ipynb`
   - `03b_ranking_contribution.ipynb`
   - `03c_customer_behavior.ipynb`
   - `03d_advanced_analytics.ipynb`
   - `04_recommendation_system.ipynb`

## Notes

- The SQL notebooks rely on DuckDB file locking. If you have multiple notebooks running at the same time, you may see a DuckDB “conflicting lock” error. Restart the kernel / stop other running kernels before re-running cells.
- `ecommerce.db` is committed in this repo so you can explore results immediately. If you prefer regenerating the data from scratch, re-run `01_setup_and_data.ipynb`.

