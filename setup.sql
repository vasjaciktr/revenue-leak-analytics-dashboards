-- Leakonic v1 setup.sql
-- Replace YOUR_PROJECT and YOUR_GA4_DATASET before running.

DECLARE start_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY));
DECLARE end_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

-- 1. Landing page performance

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.landing_pages_performance` AS
WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    user_pseudo_id,
    CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,
    event_name,
    device.category AS device_category,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    ecommerce.transaction_id AS transaction_id,
    ecommerce.purchase_revenue AS purchase_revenue
  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

session_landing AS (
  SELECT
    session_id,
    ANY_VALUE(user_pseudo_id) AS user_pseudo_id,
    ARRAY_AGG(page_location IGNORE NULLS ORDER BY date LIMIT 1)[SAFE_OFFSET(0)] AS landing_page,
    ANY_VALUE(device_category) AS device_category,
    ANY_VALUE(source) AS source,
    ANY_VALUE(medium) AS medium
  FROM base
  WHERE session_id IS NOT NULL
  GROUP BY session_id
),

purchases AS (
  SELECT
    session_id,
    COUNT(DISTINCT transaction_id) AS transactions,
    SUM(IFNULL(purchase_revenue, 0)) AS revenue
  FROM base
  WHERE event_name = 'purchase'
  GROUP BY session_id
)

SELECT
  landing_page,
  COUNT(DISTINCT s.session_id) AS sessions,
  COUNT(DISTINCT s.user_pseudo_id) AS users,
  SUM(IFNULL(p.transactions, 0)) AS transactions,
  SUM(IFNULL(p.revenue, 0)) AS revenue,
  SAFE_DIVIDE(SUM(IFNULL(p.transactions, 0)), COUNT(DISTINCT s.session_id)) AS conversion_rate,
  SAFE_DIVIDE(SUM(IFNULL(p.revenue, 0)), COUNT(DISTINCT s.session_id)) AS revenue_per_session
FROM session_landing s
LEFT JOIN purchases p
  ON s.session_id = p.session_id
WHERE landing_page IS NOT NULL
GROUP BY landing_page;


-- 2. Device performance

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.device_performance` AS
WITH base AS (
  SELECT
    user_pseudo_id,
    CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,
    event_name,
    device.category AS device_category,
    ecommerce.transaction_id AS transaction_id,
    ecommerce.purchase_revenue AS purchase_revenue
  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

sessions AS (
  SELECT
    session_id,
    ANY_VALUE(device_category) AS device_category
  FROM base
  WHERE session_id IS NOT NULL
  GROUP BY session_id
),

purchases AS (
  SELECT
    session_id,
    COUNT(DISTINCT transaction_id) AS transactions,
    SUM(IFNULL(purchase_revenue, 0)) AS revenue
  FROM base
  WHERE event_name = 'purchase'
  GROUP BY session_id
)

SELECT
  s.device_category,
  COUNT(DISTINCT s.session_id) AS sessions,
  SUM(IFNULL(p.transactions, 0)) AS transactions,
  SUM(IFNULL(p.revenue, 0)) AS revenue,
  SAFE_DIVIDE(SUM(IFNULL(p.transactions, 0)), COUNT(DISTINCT s.session_id)) AS conversion_rate,
  SAFE_DIVIDE(SUM(IFNULL(p.revenue, 0)), COUNT(DISTINCT s.session_id)) AS revenue_per_session
FROM sessions s
LEFT JOIN purchases p
  ON s.session_id = p.session_id
GROUP BY s.device_category;


-- 3. Validation checks

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.validation_checks` AS
WITH base AS (
  SELECT
    event_name,
    ecommerce.transaction_id AS transaction_id,
    ecommerce.purchase_revenue AS purchase_revenue,
    ARRAY_LENGTH(items) AS item_count
  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
)

SELECT 'purchase_events_exist' AS check_name,
       IF(COUNTIF(event_name = 'purchase') > 0, 'ok', 'fail') AS status,
       COUNTIF(event_name = 'purchase') AS value
FROM base

UNION ALL

SELECT 'purchase_revenue_exists',
       IF(SUM(IFNULL(purchase_revenue, 0)) > 0, 'ok', 'warning'),
       SUM(IFNULL(purchase_revenue, 0))
FROM base
WHERE event_name = 'purchase'

UNION ALL

SELECT 'transaction_ids_exist',
       IF(COUNTIF(event_name = 'purchase' AND transaction_id IS NOT NULL) > 0, 'ok', 'fail'),
       COUNTIF(event_name = 'purchase' AND transaction_id IS NOT NULL)
FROM base

UNION ALL

SELECT 'purchase_items_exist',
       IF(COUNTIF(event_name = 'purchase' AND item_count > 0) > 0, 'ok', 'warning'),
       COUNTIF(event_name = 'purchase' AND item_count > 0)
FROM base;


-- 4. Leak signals

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.signals` AS
WITH site_avg AS (
  SELECT
    AVG(conversion_rate) AS avg_page_cr
  FROM `YOUR_PROJECT.leakonic.landing_pages_performance`
  WHERE sessions >= 50
),

landing_signals AS (
  SELECT
    landing_page AS entity,
    'landing_page' AS entity_type,
    'high_traffic_low_conversion' AS signal_type,
    CASE
      WHEN sessions >= 1000 AND conversion_rate < avg_page_cr * 0.5 THEN 'high'
      WHEN sessions >= 300 AND conversion_rate < avg_page_cr * 0.5 THEN 'medium'
      ELSE 'low'
    END AS severity,
    sessions,
    transactions,
    revenue,
    conversion_rate,
    revenue_per_session,
    CONCAT(
      'This landing page has traffic but converts significantly below the site average. Sessions: ',
      CAST(sessions AS STRING),
      ', CR: ',
      CAST(ROUND(conversion_rate * 100, 2) AS STRING),
      '%.'
    ) AS interpretation
  FROM `YOUR_PROJECT.leakonic.landing_pages_performance`, site_avg
  WHERE sessions >= 300
    AND conversion_rate < avg_page_cr * 0.5
),

no_revenue_signals AS (
  SELECT
    landing_page AS entity,
    'landing_page' AS entity_type,
    'traffic_no_revenue' AS signal_type,
    CASE
      WHEN sessions >= 1000 THEN 'high'
      WHEN sessions >= 300 THEN 'medium'
      ELSE 'low'
    END AS severity,
    sessions,
    transactions,
    revenue,
    conversion_rate,
    revenue_per_session,
    CONCAT(
      'This landing page receives traffic but generated no revenue in the selected period. Sessions: ',
      CAST(sessions AS STRING),
      '.'
    ) AS interpretation
  FROM `YOUR_PROJECT.leakonic.landing_pages_performance`
  WHERE sessions >= 300
    AND IFNULL(revenue, 0) = 0
),

device_gap AS (
  SELECT
    'mobile_vs_desktop' AS entity,
    'device' AS entity_type,
    'mobile_conversion_gap' AS signal_type,
    CASE
      WHEN mobile.conversion_rate < desktop.conversion_rate * 0.4 THEN 'high'
      WHEN mobile.conversion_rate < desktop.conversion_rate * 0.7 THEN 'medium'
      ELSE 'low'
    END AS severity,
    mobile.sessions AS sessions,
    mobile.transactions AS transactions,
    mobile.revenue AS revenue,
    mobile.conversion_rate AS conversion_rate,
    mobile.revenue_per_session AS revenue_per_session,
    CONCAT(
      'Mobile conversion rate is lower than desktop. Mobile CR: ',
      CAST(ROUND(mobile.conversion_rate * 100, 2) AS STRING),
      '%, desktop CR: ',
      CAST(ROUND(desktop.conversion_rate * 100, 2) AS STRING),
      '%.'
    ) AS interpretation
  FROM `YOUR_PROJECT.leakonic.device_performance` mobile
  JOIN `YOUR_PROJECT.leakonic.device_performance` desktop
    ON mobile.device_category = 'mobile'
   AND desktop.device_category = 'desktop'
  WHERE mobile.sessions >= 300
    AND desktop.sessions >= 300
    AND mobile.conversion_rate < desktop.conversion_rate * 0.7
)

SELECT * FROM landing_signals
UNION ALL
SELECT * FROM no_revenue_signals
UNION ALL
SELECT * FROM device_gap;
