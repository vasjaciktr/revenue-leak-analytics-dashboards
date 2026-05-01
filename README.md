# Leakonic
A data analytics starter pack that allows data-rich but insight-poor companies quickly and easily get revenue leak issues and SEO insights combining data from GA4 and Google Search Console (optionally) in one place.

## Starter
**Revenue leaks**
- Landing page performance
- Performance by device
- Leak signals (high_traffic_low_conversion, mobile_gap, no_revenue_pages)
- Funnel performance
## Growth
**Revenue leaks by device + Data validation + SEO + GEO**
- Landing page performance
- Performance by device
- Leak signals (high_traffic_low_conversion, mobile_gap, no_revenue_pages)
- Funnel performance
- Funnel performance by device
- Data validation
- Search engine optimization (SEO)
## Pro
**Revenue leaks by device + Data validation + SEO + GEO + Product Analysis**
- Landing page performance
- Performance by device
- Leak signals (high_traffic_low_conversion, mobile_gap, no_revenue_pages)
- Funnel performance
- Funnel performance by device
- Data validation
- Search engine optimization (SEO)
- Generative engine optimization (GEO)
- Product analysis

# Core
**It shows where you're losing revenue.**
starter-setup.sql will create the following tables:

## landing_pages_performance
It shows "where there is traffic but there's no conversion"

## device_performance
It highlights “mobile vs desktop issues”

## leak_signals
first 3–5 signals:
- high_traffic_low_conversion
- mobile_gap
- no_revenue_pages

## validation_checks (minimal)
It checks whether data coming from GA4 is correct

# Growth
**It shows how to improve and optimize.**
setup.sql will create the following tables:

**Then a client runs a script, get the tables, and opens the dashboard**
