-- Leakonic Core v1 starter-setup.sql

-- IMPORTANT: Replace YOUR_PROJECT with Your Project Name
-- IMPORTANT: Replace YOUR_GA4_DATASET with Your GA4 Dataset name


DECLARE start_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY));
DECLARE end_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));


-- 0. Create dataset (schema)

CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.leakonic`;


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


-- 3. Adaptive funnel performance

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.funnel_performance` AS
WITH base AS (
  SELECT
    user_pseudo_id,
    CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,
    event_name
  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

steps AS (
  SELECT
    session_id,

    MAX(IF(event_name = 'session_start', 1, 0)) AS session_start,
    MAX(IF(event_name = 'view_item_list', 1, 0)) AS view_item_list,
    MAX(IF(event_name = 'view_item', 1, 0)) AS view_item,
    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS add_to_cart,
    MAX(IF(event_name = 'view_cart', 1, 0)) AS view_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS begin_checkout,
    MAX(IF(event_name = 'add_shipping_info', 1, 0)) AS add_shipping_info,
    MAX(IF(event_name = 'add_payment_info', 1, 0)) AS add_payment_info,
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchase

  FROM base
  WHERE session_id IS NOT NULL
  GROUP BY session_id
),

counts AS (
  SELECT
    COUNT(*) AS total_sessions,

    COUNTIF(session_start = 1) AS session_start_sessions,
    COUNTIF(view_item_list = 1) AS view_item_list_sessions,
    COUNTIF(view_item = 1) AS view_item_sessions,
    COUNTIF(add_to_cart = 1) AS add_to_cart_sessions,
    COUNTIF(view_cart = 1) AS view_cart_sessions,
    COUNTIF(begin_checkout = 1) AS begin_checkout_sessions,
    COUNTIF(add_shipping_info = 1) AS add_shipping_info_sessions,
    COUNTIF(add_payment_info = 1) AS add_payment_info_sessions,
    COUNTIF(purchase = 1) AS purchase_sessions
  FROM steps
),

funnel_rows AS (
  SELECT 1 AS step_number, 'session_start' AS step_name, TRUE AS is_required, session_start_sessions AS sessions FROM counts
  UNION ALL
  SELECT 2, 'view_item_list', FALSE, view_item_list_sessions FROM counts WHERE view_item_list_sessions > 0
  UNION ALL
  SELECT 3, 'view_item', FALSE, view_item_sessions FROM counts WHERE view_item_sessions > 0
  UNION ALL
  SELECT 4, 'add_to_cart', TRUE, add_to_cart_sessions FROM counts
  UNION ALL
  SELECT 5, 'view_cart', FALSE, view_cart_sessions FROM counts WHERE view_cart_sessions > 0
  UNION ALL
  SELECT 6, 'begin_checkout', TRUE, begin_checkout_sessions FROM counts
  UNION ALL
  SELECT 7, 'add_shipping_info', FALSE, add_shipping_info_sessions FROM counts WHERE add_shipping_info_sessions > 0
  UNION ALL
  SELECT 8, 'add_payment_info', FALSE, add_payment_info_sessions FROM counts WHERE add_payment_info_sessions > 0
  UNION ALL
  SELECT 9, 'purchase', TRUE, purchase_sessions FROM counts
)

SELECT
  step_number,
  step_name,
  is_required,
  sessions,

  LAG(sessions) OVER (ORDER BY step_number) AS previous_visible_step_sessions,

  SAFE_DIVIDE(
    sessions,
    LAG(sessions) OVER (ORDER BY step_number)
  ) AS step_conversion_rate,

  1 - SAFE_DIVIDE(
    sessions,
    LAG(sessions) OVER (ORDER BY step_number)
  ) AS step_dropoff_rate

FROM funnel_rows
ORDER BY step_number;


-- 4. Adaptive funnel by device

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.funnel_by_device` AS
WITH base AS (
  SELECT
    user_pseudo_id,
    CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,
    event_name,
    device.category AS device_category
  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

steps AS (
  SELECT
    session_id,
    ANY_VALUE(device_category) AS device_category,

    MAX(IF(event_name = 'session_start', 1, 0)) AS session_start,
    MAX(IF(event_name = 'view_item_list', 1, 0)) AS view_item_list,
    MAX(IF(event_name = 'view_item', 1, 0)) AS view_item,
    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS add_to_cart,
    MAX(IF(event_name = 'view_cart', 1, 0)) AS view_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS begin_checkout,
    MAX(IF(event_name = 'add_shipping_info', 1, 0)) AS add_shipping_info,
    MAX(IF(event_name = 'add_payment_info', 1, 0)) AS add_payment_info,
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchase

  FROM base
  WHERE session_id IS NOT NULL
  GROUP BY session_id
),

counts AS (
  SELECT
    device_category,

    COUNT(*) AS total_sessions,

    COUNTIF(session_start = 1) AS session_start_sessions,
    COUNTIF(view_item_list = 1) AS view_item_list_sessions,
    COUNTIF(view_item = 1) AS view_item_sessions,
    COUNTIF(add_to_cart = 1) AS add_to_cart_sessions,
    COUNTIF(view_cart = 1) AS view_cart_sessions,
    COUNTIF(begin_checkout = 1) AS begin_checkout_sessions,
    COUNTIF(add_shipping_info = 1) AS add_shipping_info_sessions,
    COUNTIF(add_payment_info = 1) AS add_payment_info_sessions,
    COUNTIF(purchase = 1) AS purchase_sessions
  FROM steps
  GROUP BY device_category
),

funnel_rows AS (
  SELECT device_category, 1 AS step_number, 'session_start' AS step_name, TRUE AS is_required, session_start_sessions AS sessions FROM counts
  UNION ALL
  SELECT device_category, 2, 'view_item_list', FALSE, view_item_list_sessions FROM counts WHERE view_item_list_sessions > 0
  UNION ALL
  SELECT device_category, 3, 'view_item', FALSE, view_item_sessions FROM counts WHERE view_item_sessions > 0
  UNION ALL
  SELECT device_category, 4, 'add_to_cart', TRUE, add_to_cart_sessions FROM counts
  UNION ALL
  SELECT device_category, 5, 'view_cart', FALSE, view_cart_sessions FROM counts WHERE view_cart_sessions > 0
  UNION ALL
  SELECT device_category, 6, 'begin_checkout', TRUE, begin_checkout_sessions FROM counts
  UNION ALL
  SELECT device_category, 7, 'add_shipping_info', FALSE, add_shipping_info_sessions FROM counts WHERE add_shipping_info_sessions > 0
  UNION ALL
  SELECT device_category, 8, 'add_payment_info', FALSE, add_payment_info_sessions FROM counts WHERE add_payment_info_sessions > 0
  UNION ALL
  SELECT device_category, 9, 'purchase', TRUE, purchase_sessions FROM counts
)

SELECT
  device_category,
  step_number,
  step_name,
  is_required,
  sessions,

  LAG(sessions) OVER (
    PARTITION BY device_category
    ORDER BY step_number
  ) AS previous_visible_step_sessions,

  SAFE_DIVIDE(
    sessions,
    LAG(sessions) OVER (
      PARTITION BY device_category
      ORDER BY step_number
    )
  ) AS step_conversion_rate,

  1 - SAFE_DIVIDE(
    sessions,
    LAG(sessions) OVER (
      PARTITION BY device_category
      ORDER BY step_number
    )
  ) AS step_dropoff_rate

FROM funnel_rows
ORDER BY device_category, step_number;



-- X. Funnel sessions (new logic)

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.funnel_sessions` AS

WITH base AS (
  SELECT
    CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,

    event_name

  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
)

SELECT
  session_id,

  MAX(IF(event_name = 'view_item_list', 1, 0)) AS view_item_list,
  MAX(IF(event_name = 'view_item', 1, 0)) AS view_item,
  MAX(IF(event_name = 'add_to_cart', 1, 0)) AS add_to_cart,
  MAX(IF(event_name = 'view_cart', 1, 0)) AS view_cart,
  MAX(IF(event_name = 'begin_checkout', 1, 0)) AS begin_checkout,
  MAX(IF(event_name = 'add_payment_info', 1, 0)) AS add_payment_info,
  MAX(IF(event_name = 'purchase', 1, 0)) AS purchase

FROM base
WHERE session_id IS NOT NULL
GROUP BY session_id;


-- X. Funnel transitions

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.funnel_transitions` AS

WITH base AS (
  SELECT *
  FROM `YOUR_PROJECT.leakonic.cart_sessions`
),

transitions_raw AS (

  SELECT
    'add_to_cart' AS from_step,
    'view_cart' AS to_step,
    COUNTIF(add_to_cart = 1) AS from_sessions,
    COUNTIF(add_to_cart = 1 AND view_cart = 1) AS to_sessions,
    SUM(IF(add_to_cart = 1 AND view_cart = 0 AND purchase = 0, add_to_cart_value, 0)) AS lost_revenue
  FROM base

  UNION ALL

  SELECT
    'view_cart',
    'begin_checkout',
    COUNTIF(view_cart = 1),
    COUNTIF(view_cart = 1 AND begin_checkout = 1),
    SUM(IF(view_cart = 1 AND begin_checkout = 0 AND purchase = 0, IFNULL(view_cart_value, add_to_cart_value), 0))
  FROM base

  UNION ALL

  SELECT
  'begin_checkout',
  'add_shipping_info',
  COUNTIF(begin_checkout = 1),
  COUNTIF(begin_checkout = 1 AND add_shipping_info = 1),
  SUM(
    IF(
      begin_checkout = 1
      AND add_shipping_info = 0
      AND purchase = 0,
      IFNULL(begin_checkout_value, add_to_cart_value),
      0
    )
  )
FROM base

UNION ALL

-- add_shipping_info → add_payment_info
SELECT
  'add_shipping_info',
  'add_payment_info',
  COUNTIF(add_shipping_info = 1),
  COUNTIF(add_shipping_info = 1 AND add_payment_info = 1),
  SUM(
    IF(
      add_shipping_info = 1
      AND add_payment_info = 0
      AND purchase = 0,
      IFNULL(add_shipping_info_value, begin_checkout_value),
      0
    )
  )
FROM base

  UNION ALL

  SELECT
    'add_payment_info',
    'purchase',
    COUNTIF(add_payment_info = 1),
    COUNTIF(add_payment_info = 1 AND purchase = 1),
    SUM(IF(add_payment_info = 1 AND purchase = 0, IFNULL(add_payment_info_value, begin_checkout_value), 0))
  FROM base
)

SELECT
  from_step,
  to_step,
  from_sessions,
  to_sessions,
  from_sessions - to_sessions AS dropoff_sessions,
  lost_revenue,
  SAFE_DIVIDE(to_sessions, from_sessions) AS conversion_rate,
  1 - SAFE_DIVIDE(to_sessions, from_sessions) AS dropoff_rate

FROM transitions_raw;


-- X. Cart sessions (with value)

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.cart_sessions` AS

WITH base AS (
  SELECT
    CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,

    event_timestamp,
  
    event_name,

    (
      SELECT
        SUM(IFNULL(item.price, 0) * IFNULL(item.quantity, 1))
      FROM UNNEST(items) AS item
    ) AS event_value

  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

aggregated AS (
  SELECT
    session_id,

    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS add_to_cart,
    MAX(IF(event_name = 'view_cart', 1, 0)) AS view_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS begin_checkout,
    MAX(IF(event_name = 'add_shipping_info', 1, 0)) AS add_shipping_info,
    MAX(IF(event_name = 'add_payment_info', 1, 0)) AS add_payment_info,
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchase,

    SUM(IF(event_name = 'add_to_cart', IFNULL(event_value, 0), 0)) AS add_to_cart_value,
    ARRAY_AGG(
  IF(event_name = 'view_cart', event_value, NULL)
  IGNORE NULLS
  ORDER BY event_timestamp DESC
  LIMIT 1
)[SAFE_OFFSET(0)] AS view_cart_value,
    ARRAY_AGG(
  IF(event_name = 'begin_checkout', event_value, NULL)
  IGNORE NULLS
  ORDER BY event_timestamp DESC
  LIMIT 1
)[SAFE_OFFSET(0)] AS begin_checkout_value,
    ARRAY_AGG(
  IF(event_name = 'add_shipping_info', event_value, NULL)
  IGNORE NULLS
  ORDER BY event_timestamp DESC
  LIMIT 1
)[SAFE_OFFSET(0)] AS add_shipping_info_value,
    ARRAY_AGG(
  IF(event_name = 'add_payment_info', event_value, NULL)
  IGNORE NULLS
  ORDER BY event_timestamp DESC
  LIMIT 1
)[SAFE_OFFSET(0)] AS add_payment_info_value

  FROM base
  WHERE session_id IS NOT NULL
  GROUP BY session_id
)

SELECT *
FROM aggregated
WHERE add_to_cart = 1;


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

-- 6. Overview

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.site_overview` AS
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS date,

  COUNT(DISTINCT CONCAT(
    user_pseudo_id,
    '-',
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
  )) AS sessions,

  COUNT(DISTINCT ecommerce.transaction_id) AS transactions,

  SUM(IFNULL(ecommerce.purchase_revenue, 0)) AS revenue,

  SAFE_DIVIDE(
    COUNT(DISTINCT ecommerce.transaction_id),
    COUNT(DISTINCT CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ))
  ) AS conversion_rate

FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
GROUP BY date;


-- 7. Signals

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
    CONCAT(from_step, '_to_', to_step) AS entity,
    'funnel' AS entity_type,
    CONCAT(from_step, '_to_', to_step, '_dropoff') AS signal_type,

    CASE
      WHEN dropoff_rate >= 0.7 THEN 'high'
      WHEN dropoff_rate >= 0.5 THEN 'medium'
      WHEN dropoff_rate >= 0.3 THEN 'low'
    END AS severity,

    from_sessions AS sessions,
    NULL AS transactions,
    lost_revenue AS revenue,
    dropoff_rate AS metric_value,

    CASE
      WHEN from_step = 'add_to_cart' AND to_step = 'view_cart'
        THEN CONCAT(
          'Users add products to cart but do not open the cart. Estimated lost revenue: ',
          CAST(ROUND(lost_revenue, 2) AS STRING)
        )

      WHEN from_step = 'view_cart' AND to_step = 'begin_checkout'
        THEN CONCAT(
          'Users open the cart but do not proceed to checkout. Estimated lost revenue: ',
          CAST(ROUND(lost_revenue, 2) AS STRING)
        )

      WHEN from_step = 'begin_checkout' AND to_step = 'add_shipping_info'
  THEN CONCAT(
    'Users start checkout but do not enter shipping details. Lost revenue: ',
    CAST(ROUND(lost_revenue, 2) AS STRING)
  )

WHEN from_step = 'add_shipping_info' AND to_step = 'add_payment_info'
  THEN CONCAT(
    'Users enter shipping details but do not proceed to payment. Lost revenue: ',
    CAST(ROUND(lost_revenue, 2) AS STRING)
  )

      WHEN from_step = 'add_payment_info' AND to_step = 'purchase'
        THEN CONCAT(
          'Users reach the payment step but do not complete purchase. Estimated lost revenue: ',
          CAST(ROUND(lost_revenue, 2) AS STRING)
        )

      ELSE CONCAT(
        'Users drop off between ',
        from_step,
        ' and ',
        to_step,
        '. Estimated lost revenue: ',
        CAST(ROUND(lost_revenue, 2) AS STRING)
      )
    END AS interpretation

  FROM `YOUR_PROJECT.leakonic.funnel_transitions`

  WHERE from_sessions >= 50
    AND dropoff_rate >= 0.3
)

FROM (
  SELECT * FROM landing_signals
  UNION ALL SELECT * FROM no_revenue_signals
  UNION ALL SELECT * FROM device_gap
  UNION ALL SELECT * FROM funnel_signals
);
