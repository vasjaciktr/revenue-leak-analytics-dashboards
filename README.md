# Leakonic
A data analytics starter pack that allows data-rich but insight-poor companies quickly and easily get revenue leak issues combining data from GA4 and Google Search Console (optionally) in one place.

***Structure:***
## Starter
- Core
  * Landing page performance
  * Performance by device
  * Leak signals (high_traffic_low_conversion, mobile_gap, no_revenue_pages)
- Funnel Module
## Growth
- Core
  * Landing page performance
  * Performance by device
  * Leak signals (high_traffic_low_conversion, mobile_gap, no_revenue_pages)
- Funnel Module
- SEO Module
## Pro
- All inlusive

# Core
**It shows where you're losing revenue.**
setup.sql will create the following tables:

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
