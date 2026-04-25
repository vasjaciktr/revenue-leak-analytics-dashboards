-- Leakonic Core v1 starter-setup.sql

-- IMPORTANT: Replace YOUR_PROJECT with Your Project Name
-- IMPORTANT: Replace location="EU" with yYour GA4 Dataset Region (EU/US)

-- 0. Create dataset (schema)

CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.leakonic`
OPTIONS(location="EU");

DECLARE start_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY));
DECLARE end_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

-- 1. Landing pages performance

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.landing_pages_performance` AS
WITH base AS (
  SELECT
    user_pseudo_id,
    CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id,
    event_name,
    device.category AS device_category,
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
    ARRAY_AGG(page_location IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS landing_page
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
LEFT JOIN purchases p ON s.session_id = p.session_id
WHERE landing_page IS NOT NULL
GROUP BY landing_page;


-- 2. Device performance

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.device_performance` AS
WITH base AS (
  SELECT
    user_pseudo_id,
    CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id,
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
LEFT JOIN purchases p ON s.session_id = p.session_id
GROUP BY s.device_category;


-- 3. Funnel performance

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.funnel_performance` AS
WITH base AS (
  SELECT
    user_pseudo_id,
    CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id,
    event_name
  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

steps AS (
  SELECT
    session_id,
    MAX(IF(event_name = 'view_item', 1, 0)) AS view_item,
    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS add_to_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS begin_checkout,
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchase
  FROM base
  WHERE session_id IS NOT NULL
  GROUP BY session_id
)

SELECT
  COUNTIF(view_item = 1) AS view_item_sessions,
  COUNTIF(add_to_cart = 1) AS add_to_cart_sessions,
  COUNTIF(begin_checkout = 1) AS begin_checkout_sessions,
  COUNTIF(purchase = 1) AS purchase_sessions,
  SAFE_DIVIDE(COUNTIF(add_to_cart = 1), COUNTIF(view_item = 1)) AS view_to_cart_rate,
  SAFE_DIVIDE(COUNTIF(begin_checkout = 1), COUNTIF(add_to_cart = 1)) AS cart_to_checkout_rate,
  SAFE_DIVIDE(COUNTIF(purchase = 1), COUNTIF(begin_checkout = 1)) AS checkout_to_purchase_rate
FROM steps;


-- 4. Funnel by device

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.funnel_by_device` AS
WITH base AS (
  SELECT
    user_pseudo_id,
    CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id,
    event_name,
    device.category AS device_category
  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

steps AS (
  SELECT
    session_id,
    ANY_VALUE(device_category) AS device_category,
    MAX(IF(event_name = 'view_item', 1, 0)) AS view_item,
    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS add_to_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS begin_checkout,
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchase
  FROM base
  WHERE session_id IS NOT NULL
  GROUP BY session_id
)

SELECT
  device_category,
  COUNTIF(view_item = 1) AS view_item_sessions,
  COUNTIF(add_to_cart = 1) AS add_to_cart_sessions,
  COUNTIF(begin_checkout = 1) AS begin_checkout_sessions,
  COUNTIF(purchase = 1) AS purchase_sessions,
  SAFE_DIVIDE(COUNTIF(add_to_cart = 1), COUNTIF(view_item = 1)) AS view_to_cart_rate,
  SAFE_DIVIDE(COUNTIF(begin_checkout = 1), COUNTIF(add_to_cart = 1)) AS cart_to_checkout_rate,
  SAFE_DIVIDE(COUNTIF(purchase = 1), COUNTIF(begin_checkout = 1)) AS checkout_to_purchase_rate
FROM steps
GROUP BY device_category;


-- 5. Validation checks

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


-- 6. Signals

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.signals` AS
WITH site_avg AS (
  SELECT AVG(conversion_rate) AS avg_page_cr
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
    conversion_rate AS metric_value,
    CONCAT('Page has traffic but converts below site average. Sessions: ', CAST(sessions AS STRING)) AS interpretation
  FROM `YOUR_PROJECT.leakonic.landing_pages_performance`, site_avg
  WHERE sessions >= 300
    AND conversion_rate < avg_page_cr * 0.5
),

no_revenue_signals AS (
  SELECT
    landing_page,
    'landing_page',
    'traffic_no_revenue',
    CASE
      WHEN sessions >= 1000 THEN 'high'
      WHEN sessions >= 300 THEN 'medium'
      ELSE 'low'
    END,
    sessions,
    transactions,
    revenue,
    revenue_per_session,
    CONCAT('Page receives traffic but generated no revenue. Sessions: ', CAST(sessions AS STRING))
  FROM `YOUR_PROJECT.leakonic.landing_pages_performance`
  WHERE sessions >= 300
    AND IFNULL(revenue, 0) = 0
),

device_gap AS (
  SELECT
    'mobile_vs_desktop',
    'device',
    'mobile_conversion_gap',
    CASE
      WHEN m.conversion_rate < d.conversion_rate * 0.6 THEN 'high'
      WHEN m.conversion_rate < d.conversion_rate * 0.8 THEN 'medium'
      ELSE 'low'
    END,
    m.sessions,
    m.transactions,
    m.revenue,
    m.conversion_rate,
    CONCAT('Mobile CR is lower than desktop. Mobile CR: ', CAST(ROUND(m.conversion_rate * 100, 2) AS STRING), '%, desktop CR: ', CAST(ROUND(d.conversion_rate * 100, 2) AS STRING), '%.')
  FROM `YOUR_PROJECT.leakonic.device_performance` m
  JOIN `YOUR_PROJECT.leakonic.device_performance` d
    ON m.device_category = 'mobile'
   AND d.device_category = 'desktop'
  WHERE m.sessions >= 300
    AND d.sessions >= 300
    AND m.conversion_rate < d.conversion_rate * 0.8
),

funnel_signals AS (
  SELECT
    'funnel',
    'funnel',
    'low_add_to_cart_rate',
    CASE
      WHEN view_to_cart_rate < 0.03 THEN 'high'
      WHEN view_to_cart_rate < 0.06 THEN 'medium'
      ELSE 'low'
    END,
    view_item_sessions,
    NULL,
    NULL,
    view_to_cart_rate,
    'Users view products but rarely add them to cart.'
  FROM `YOUR_PROJECT.leakonic.funnel_performance`
  WHERE view_item_sessions >= 300
    AND view_to_cart_rate < 0.06

  UNION ALL

  SELECT
    'funnel',
    'funnel',
    'cart_to_checkout_dropoff',
    CASE
      WHEN cart_to_checkout_rate < 0.3 THEN 'high'
      WHEN cart_to_checkout_rate < 0.5 THEN 'medium'
      ELSE 'low'
    END,
    add_to_cart_sessions,
    NULL,
    NULL,
    cart_to_checkout_rate,
    'Users add items to cart but do not start checkout.'
  FROM `YOUR_PROJECT.leakonic.funnel_performance`
  WHERE add_to_cart_sessions >= 100
    AND cart_to_checkout_rate < 0.5

  UNION ALL

  SELECT
    'funnel',
    'funnel',
    'checkout_dropoff',
    CASE
      WHEN checkout_to_purchase_rate < 0.3 THEN 'high'
      WHEN checkout_to_purchase_rate < 0.5 THEN 'medium'
      ELSE 'low'
    END,
    begin_checkout_sessions,
    NULL,
    NULL,
    checkout_to_purchase_rate,
    'Users start checkout but do not complete purchase.'
  FROM `YOUR_PROJECT.leakonic.funnel_performance`
  WHERE begin_checkout_sessions >= 100
    AND checkout_to_purchase_rate < 0.5
)

SELECT * FROM landing_signals
UNION ALL SELECT * FROM no_revenue_signals
UNION ALL SELECT * FROM device_gap
UNION ALL SELECT * FROM funnel_signals;
