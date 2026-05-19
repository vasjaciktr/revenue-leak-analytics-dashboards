-- Leakonic Core v1 medium-setup.sql

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


-- 3. Cart sessions

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
    COALESCE(value.double_value, value.int_value)
  FROM UNNEST(event_params)
  WHERE key = 'value'
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


-- 4. Funnel Analysis by Device
-- It creates:
-- 4A. funnel_steps_by_device
-- 4B. funnel_transitions_by_device
-- 4C. device_funnel_priority


-- 4A. Full funnel steps by device

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.funnel_steps_by_device` AS

WITH base AS (
  SELECT
    CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,

    CASE
      WHEN LOWER(device.category) IN ('desktop', 'mobile', 'tablet')
        THEN LOWER(device.category)
      ELSE 'other'
    END AS device_category,

    event_name

  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

session_steps AS (
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

funnel_steps AS (
  SELECT 1 AS step_order, 'session_start' AS step_name, 'Session started' AS step_label UNION ALL
  SELECT 2, 'view_item_list', 'Viewed product list' UNION ALL
  SELECT 3, 'view_item', 'Viewed product' UNION ALL
  SELECT 4, 'add_to_cart', 'Added to cart' UNION ALL
  SELECT 5, 'view_cart', 'Viewed cart' UNION ALL
  SELECT 6, 'begin_checkout', 'Started checkout' UNION ALL
  SELECT 7, 'add_shipping_info', 'Added shipping info' UNION ALL
  SELECT 8, 'add_payment_info', 'Added payment info' UNION ALL
  SELECT 9, 'purchase', 'Purchased'
),

step_counts AS (
  SELECT
    s.device_category,
    f.step_order,
    f.step_name,
    f.step_label,

    COUNT(DISTINCT CASE
      WHEN f.step_name = 'session_start' AND s.session_start = 1 THEN s.session_id
      WHEN f.step_name = 'view_item_list' AND s.view_item_list = 1 THEN s.session_id
      WHEN f.step_name = 'view_item' AND s.view_item = 1 THEN s.session_id
      WHEN f.step_name = 'add_to_cart' AND s.add_to_cart = 1 THEN s.session_id
      WHEN f.step_name = 'view_cart' AND s.view_cart = 1 THEN s.session_id
      WHEN f.step_name = 'begin_checkout' AND s.begin_checkout = 1 THEN s.session_id
      WHEN f.step_name = 'add_shipping_info' AND s.add_shipping_info = 1 THEN s.session_id
      WHEN f.step_name = 'add_payment_info' AND s.add_payment_info = 1 THEN s.session_id
      WHEN f.step_name = 'purchase' AND s.purchase = 1 THEN s.session_id
    END) AS sessions

  FROM session_steps s
  CROSS JOIN funnel_steps f
  GROUP BY
    s.device_category,
    f.step_order,
    f.step_name,
    f.step_label
),

final AS (
  SELECT
    *,
    FIRST_VALUE(sessions) OVER (
      PARTITION BY device_category
      ORDER BY step_order
    ) AS starting_sessions,

    LAG(sessions) OVER (
      PARTITION BY device_category
      ORDER BY step_order
    ) AS previous_step_sessions

  FROM step_counts
)

SELECT
  device_category,
  step_order,
  step_name,
  step_label,
  sessions,

  SAFE_DIVIDE(sessions, starting_sessions) AS conversion_from_start,

  SAFE_DIVIDE(
    sessions,
    previous_step_sessions
  ) AS conversion_from_previous_step,

  previous_step_sessions - sessions AS dropoff_sessions,

  1 - SAFE_DIVIDE(
    sessions,
    previous_step_sessions
  ) AS dropoff_rate

FROM final
ORDER BY
  device_category,
  step_order;



-- 4B. Funnel transitions by device

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.funnel_transitions_by_device` AS

WITH base AS (
  SELECT
    CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,

    CASE
      WHEN LOWER(device.category) IN ('desktop', 'mobile', 'tablet')
        THEN LOWER(device.category)
      ELSE 'other'
    END AS device_category,

    event_timestamp,
    event_name,

    (
      SELECT COALESCE(value.double_value, value.int_value)
      FROM UNNEST(event_params)
      WHERE key = 'value'
    ) AS event_value

  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

cart_sessions_by_device AS (
  SELECT
    session_id,
    ANY_VALUE(device_category) AS device_category,

    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS add_to_cart,
    MAX(IF(event_name = 'view_cart', 1, 0)) AS view_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS begin_checkout,
    MAX(IF(event_name = 'add_shipping_info', 1, 0)) AS add_shipping_info,
    MAX(IF(event_name = 'add_payment_info', 1, 0)) AS add_payment_info,
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchase,

    SUM(
      IF(event_name = 'add_to_cart', IFNULL(event_value, 0), 0)
    ) AS add_to_cart_value,

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
),

cart_sessions_filtered AS (
  SELECT *
  FROM cart_sessions_by_device
  WHERE add_to_cart = 1
),

transitions_raw AS (

  -- add_to_cart > view_cart
  SELECT
    device_category,
    1 AS transition_order,
    'add_to_cart' AS from_step,
    'view_cart' AS to_step,
    'Added to cart → Viewed cart' AS transition_label,

    COUNTIF(add_to_cart = 1) AS from_sessions,
    COUNTIF(add_to_cart = 1 AND view_cart = 1) AS to_sessions,

    SUM(
      IF(
        add_to_cart = 1
        AND view_cart = 0
        AND purchase = 0,
        add_to_cart_value,
        0
      )
    ) AS lost_revenue

  FROM cart_sessions_filtered
  GROUP BY device_category

  UNION ALL

  -- view_cart > begin_checkout
  SELECT
    device_category,
    2 AS transition_order,
    'view_cart' AS from_step,
    'begin_checkout' AS to_step,
    'Viewed cart → Started checkout' AS transition_label,

    COUNTIF(view_cart = 1) AS from_sessions,
    COUNTIF(view_cart = 1 AND begin_checkout = 1) AS to_sessions,

    SUM(
      IF(
        view_cart = 1
        AND begin_checkout = 0
        AND purchase = 0,
        IFNULL(view_cart_value, add_to_cart_value),
        0
      )
    ) AS lost_revenue

  FROM cart_sessions_filtered
  GROUP BY device_category

  UNION ALL

  -- begin_checkout > add_shipping_info
  SELECT
    device_category,
    3 AS transition_order,
    'begin_checkout' AS from_step,
    'add_shipping_info' AS to_step,
    'Started checkout → Added shipping info' AS transition_label,

    COUNTIF(begin_checkout = 1) AS from_sessions,
    COUNTIF(begin_checkout = 1 AND add_shipping_info = 1) AS to_sessions,

    SUM(
      IF(
        begin_checkout = 1
        AND add_shipping_info = 0
        AND purchase = 0,
        IFNULL(begin_checkout_value, add_to_cart_value),
        0
      )
    ) AS lost_revenue

  FROM cart_sessions_filtered
  GROUP BY device_category

  UNION ALL

  -- add_shipping_info > add_payment_info
  SELECT
    device_category,
    4 AS transition_order,
    'add_shipping_info' AS from_step,
    'add_payment_info' AS to_step,
    'Added shipping info → Added payment info' AS transition_label,

    COUNTIF(add_shipping_info = 1) AS from_sessions,
    COUNTIF(add_shipping_info = 1 AND add_payment_info = 1) AS to_sessions,

    SUM(
      IF(
        add_shipping_info = 1
        AND add_payment_info = 0
        AND purchase = 0,
        IFNULL(add_shipping_info_value, begin_checkout_value),
        0
      )
    ) AS lost_revenue

  FROM cart_sessions_filtered
  GROUP BY device_category

  UNION ALL

  -- add_payment_info > purchase
  SELECT
    device_category,
    5 AS transition_order,
    'add_payment_info' AS from_step,
    'purchase' AS to_step,
    'Added payment info → Purchased' AS transition_label,

    COUNTIF(add_payment_info = 1) AS from_sessions,
    COUNTIF(add_payment_info = 1 AND purchase = 1) AS to_sessions,

    SUM(
      IF(
        add_payment_info = 1
        AND purchase = 0,
        IFNULL(add_payment_info_value, begin_checkout_value),
        0
      )
    ) AS lost_revenue

  FROM cart_sessions_filtered
  GROUP BY device_category
),

final AS (
  SELECT
    *,
    from_sessions - to_sessions AS dropoff_sessions,
    SAFE_DIVIDE(to_sessions, from_sessions) AS conversion_rate,
    1 - SAFE_DIVIDE(to_sessions, from_sessions) AS dropoff_rate,
    SUM(lost_revenue) OVER () AS total_lost_revenue,
    SUM(lost_revenue) OVER (PARTITION BY device_category) AS device_lost_revenue,
    SUM(lost_revenue) OVER (PARTITION BY transition_label) AS transition_lost_revenue
  FROM transitions_raw
)

SELECT
  device_category,
  transition_order,
  from_step,
  to_step,
  transition_label,

  from_sessions,
  to_sessions,
  dropoff_sessions,

  lost_revenue,

  SAFE_DIVIDE(lost_revenue, total_lost_revenue) AS share_of_total_lost_revenue,
  SAFE_DIVIDE(lost_revenue, device_lost_revenue) AS share_of_device_lost_revenue,
  SAFE_DIVIDE(lost_revenue, transition_lost_revenue) AS share_of_transition_lost_revenue,

  conversion_rate,
  dropoff_rate,

  SAFE_DIVIDE(lost_revenue, dropoff_sessions) AS avg_lost_revenue_per_dropoff_session,

  CASE
    WHEN transition_order = 1 THEN 'Medium'
    WHEN transition_order = 2 THEN 'High'
    WHEN transition_order IN (3, 4, 5) THEN 'Very high'
    ELSE 'Unknown'
  END AS revenue_confidence

FROM final
ORDER BY
  device_category,
  transition_order;



-- 4C. Device funnel priority summary

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.device_funnel_priority` AS

WITH device_leaks AS (
  SELECT
    device_category,

    SUM(lost_revenue) AS total_lost_revenue,
    SUM(dropoff_sessions) AS total_dropoff_sessions,

    ARRAY_AGG(
      STRUCT(
        transition_label,
        lost_revenue,
        dropoff_rate
      )
      ORDER BY lost_revenue DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS biggest_leak

  FROM `YOUR_PROJECT.leakonic.funnel_transitions_by_device`
  GROUP BY device_category
),

device_perf AS (
  SELECT
    device_category,
    sessions,
    transactions,
    revenue,
    conversion_rate,
    revenue_per_session
  FROM `YOUR_PROJECT.leakonic.device_performance`
),

combined AS (
  SELECT
    p.device_category,

    p.sessions,
    p.transactions,
    p.revenue,
    p.conversion_rate,
    p.revenue_per_session,

    IFNULL(l.total_lost_revenue, 0) AS total_lost_revenue,
    IFNULL(l.total_dropoff_sessions, 0) AS total_dropoff_sessions,

    l.biggest_leak.transition_label AS biggest_leak_step,
    l.biggest_leak.lost_revenue AS biggest_leak_revenue,
    l.biggest_leak.dropoff_rate AS biggest_leak_dropoff_rate,

    SAFE_DIVIDE(IFNULL(l.total_lost_revenue, 0), p.revenue) AS lost_revenue_vs_actual_revenue,
    SAFE_DIVIDE(IFNULL(l.total_lost_revenue, 0), p.sessions) AS lost_revenue_per_session

  FROM device_perf p
  LEFT JOIN device_leaks l
    ON p.device_category = l.device_category
)

SELECT
  device_category,

  sessions,
  transactions,
  revenue,
  conversion_rate,
  revenue_per_session,

  total_lost_revenue,
  total_dropoff_sessions,

  biggest_leak_step,
  biggest_leak_revenue,
  biggest_leak_dropoff_rate,

  lost_revenue_vs_actual_revenue,
  lost_revenue_per_session,

  CASE
    WHEN sessions >= 1000
      AND total_lost_revenue >= 10000
      THEN 'High'

    WHEN sessions >= 300
      AND total_lost_revenue >= 3000
      THEN 'Medium'

    WHEN total_lost_revenue > 0
      THEN 'Low'

    ELSE 'No clear leak'
  END AS priority,

  CASE
    WHEN sessions >= 1000
      AND total_lost_revenue >= 10000
      THEN CONCAT(
        'High priority: ',
        device_category,
        ' has a major revenue leak. Biggest issue: ',
        biggest_leak_step,
        '. Estimated lost revenue: ',
        CAST(ROUND(total_lost_revenue, 2) AS STRING),
        '.'
      )

    WHEN sessions >= 300
      AND total_lost_revenue >= 3000
      THEN CONCAT(
        'Medium priority: ',
        device_category,
        ' shows a meaningful funnel leak. Biggest issue: ',
        biggest_leak_step,
        '. Estimated lost revenue: ',
        CAST(ROUND(total_lost_revenue, 2) AS STRING),
        '.'
      )

    WHEN total_lost_revenue > 0
      THEN CONCAT(
        'Low priority: ',
        device_category,
        ' has some estimated leakage, but the business impact appears lower. Biggest issue: ',
        IFNULL(biggest_leak_step, 'not enough data'),
        '.'
      )

    ELSE CONCAT(
      'No clear revenue leak detected for ',
      device_category,
      '.'
    )
  END AS interpretation

FROM combined
ORDER BY
  total_lost_revenue DESC;


-- 5. Validation checks: Ecommerce event counts


CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.ecommerce_event_validation` AS

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
  COUNT(DISTINCT session_id) AS sessions,
  
  COUNTIF(event_name = 'view_item_list') AS view_item_list,
  COUNT(DISTINCT IF(event_name = 'view_item_list', session_id, NULL)) AS view_item_list_sessions,

  COUNTIF(event_name = 'view_item') AS view_item,
  COUNT(DISTINCT IF(event_name = 'view_item', session_id, NULL)) AS view_item_sessions,

  COUNTIF(event_name = 'add_to_cart') AS add_to_cart,
  COUNT(DISTINCT IF(event_name = 'add_to_cart', session_id, NULL)) AS add_to_cart_sessions,

  COUNTIF(event_name = 'view_cart') AS view_cart,
  COUNT(DISTINCT IF(event_name = 'view_cart', session_id, NULL)) AS view_cart_sessions,

  COUNTIF(event_name = 'begin_checkout') AS begin_checkout,
  COUNT(DISTINCT IF(event_name = 'begin_checkout', session_id, NULL)) AS begin_checkout_sessions,

  COUNTIF(event_name = 'add_shipping_info') AS add_shipping_info,
  COUNT(DISTINCT IF(event_name = 'add_shipping_info', session_id, NULL)) AS add_shipping_info_sessions,

  COUNTIF(event_name = 'add_payment_info') AS add_payment_info,
  COUNT(DISTINCT IF(event_name = 'add_payment_info', session_id, NULL)) AS add_payment_info_sessions,

  COUNTIF(event_name = 'purchase') AS purchase,
  COUNT(DISTINCT IF(event_name = 'purchase', session_id, NULL)) AS purchase_sessions,

  (
    IF(COUNTIF(event_name = 'view_item_list') = 0, 1, 0) +
    IF(COUNTIF(event_name = 'view_item') = 0, 1, 0) +
    IF(COUNTIF(event_name = 'add_to_cart') = 0, 1, 0) +
    IF(COUNTIF(event_name = 'view_cart') = 0, 1, 0) +
    IF(COUNTIF(event_name = 'begin_checkout') = 0, 1, 0) +
    IF(COUNTIF(event_name = 'add_shipping_info') = 0, 1, 0) +
    IF(COUNTIF(event_name = 'add_payment_info') = 0, 1, 0) +
    IF(COUNTIF(event_name = 'purchase') = 0, 1, 0)
  ) AS missing_events

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


-- 7. Signals - Medium Package
-- Enhanced with device-level funnel leaks and device priority logic

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.signals` AS

WITH site_avg AS (
  SELECT
    AVG(conversion_rate) AS avg_page_cr
  FROM `YOUR_PROJECT.leakonic.landing_pages_performance`
  WHERE sessions >= 50
),


-- 7.1 Landing pages with traffic but weak conversion

landing_signals AS (
  SELECT
    landing_page AS place,
    'landing_page' AS entity_type,
    'high_traffic_low_conversion' AS signal_type,

    CASE
      WHEN sessions >= 1000 AND conversion_rate < avg_page_cr * 0.5 THEN 'high'
      WHEN sessions >= 300 AND conversion_rate < avg_page_cr * 0.5 THEN 'medium'
      ELSE 'low'
    END AS severity,

    NULL AS device_category,
    NULL AS funnel_step,
    NULL AS priority,

    sessions,
    transactions,
    revenue AS actual_revenue,
    NULL AS lost_revenue,
    conversion_rate,
    NULL AS dropoff_rate,
    NULL AS revenue_confidence,

    CONCAT(
      'This landing page receives traffic but converts below the site average. Sessions: ',
      CAST(sessions AS STRING),
      ', conversion rate: ',
      CAST(ROUND(conversion_rate * 100, 2) AS STRING),
      '%.'
    ) AS issues,

    'Review search intent alignment, page content, trust signals, product relevance, internal links, and calls to action.' AS recommended_action

  FROM `YOUR_PROJECT.leakonic.landing_pages_performance`, site_avg
  WHERE sessions >= 300
    AND conversion_rate < avg_page_cr * 0.5
),


-- 7.2 Landing pages with traffic but no revenue

no_revenue_signals AS (
  SELECT
    landing_page AS place,
    'landing_page' AS entity_type,
    'traffic_no_revenue' AS signal_type,

    CASE
      WHEN sessions >= 1000 THEN 'high'
      WHEN sessions >= 300 THEN 'medium'
      ELSE 'low'
    END AS severity,

    NULL AS device_category,
    NULL AS funnel_step,
    NULL AS priority,

    sessions,
    transactions,
    revenue AS actual_revenue,
    NULL AS lost_revenue,
    0 AS conversion_rate,
    NULL AS dropoff_rate,
    NULL AS revenue_confidence,

    CONCAT(
      'This landing page receives traffic but generated no revenue. Sessions: ',
      CAST(sessions AS STRING),
      '.'
    ) AS issues,

    'Check whether the page targets commercial intent, links to relevant products or categories, and gives users a clear next step.' AS recommended_action

  FROM `YOUR_PROJECT.leakonic.landing_pages_performance`
  WHERE sessions >= 300
    AND IFNULL(revenue, 0) = 0
),


-- 7.3 General mobile vs desktop conversion gap
-- Kept from Starter, but Medium should not rely on this alone.

device_gap AS (

  -- Mobile worse than desktop
  SELECT
    'mobile_vs_desktop' AS place,
    'device' AS entity_type,
    'mobile_conversion_gap' AS signal_type,

    CASE
      WHEN m.conversion_rate < d.conversion_rate * 0.7 THEN 'high'
      WHEN m.conversion_rate < d.conversion_rate * 0.9 THEN 'medium'
      ELSE 'low'
    END AS severity,

    'mobile' AS device_category,
    NULL AS funnel_step,
    NULL AS priority,

    m.sessions,
    m.transactions,
    m.revenue AS actual_revenue,
    NULL AS lost_revenue,
    m.conversion_rate,
    NULL AS dropoff_rate,
    NULL AS revenue_confidence,

    CONCAT(
      'Mobile converts worse than desktop. Mobile CR: ',
      CAST(ROUND(m.conversion_rate * 100, 2) AS STRING),
      '%, desktop CR: ',
      CAST(ROUND(d.conversion_rate * 100, 2) AS STRING),
      '%.'
    ) AS issues,

    'Use the device-level funnel signals to identify the exact mobile step causing the gap.' AS recommended_action

  FROM `YOUR_PROJECT.leakonic.device_performance` m
  JOIN `YOUR_PROJECT.leakonic.device_performance` d
    ON m.device_category = 'mobile'
   AND d.device_category = 'desktop'
  WHERE m.sessions >= 300
    AND d.sessions >= 300
    AND m.conversion_rate < d.conversion_rate * 0.9

  UNION ALL

  -- Desktop worse than mobile
  SELECT
    'desktop_vs_mobile' AS place,
    'device' AS entity_type,
    'desktop_conversion_gap' AS signal_type,

    CASE
      WHEN d.conversion_rate < m.conversion_rate * 0.7 THEN 'high'
      WHEN d.conversion_rate < m.conversion_rate * 0.9 THEN 'medium'
      ELSE 'low'
    END AS severity,

    'desktop' AS device_category,
    NULL AS funnel_step,
    NULL AS priority,

    d.sessions,
    d.transactions,
    d.revenue AS actual_revenue,
    NULL AS lost_revenue,
    d.conversion_rate,
    NULL AS dropoff_rate,
    NULL AS revenue_confidence,

    CONCAT(
      'Desktop converts worse than mobile. Desktop CR: ',
      CAST(ROUND(d.conversion_rate * 100, 2) AS STRING),
      '%, mobile CR: ',
      CAST(ROUND(m.conversion_rate * 100, 2) AS STRING),
      '%.'
    ) AS issues,

    'Use the device-level funnel signals to identify the exact desktop step causing the gap.' AS recommended_action

  FROM `YOUR_PROJECT.leakonic.device_performance` m
  JOIN `YOUR_PROJECT.leakonic.device_performance` d
    ON m.device_category = 'mobile'
   AND d.device_category = 'desktop'
  WHERE m.sessions >= 300
    AND d.sessions >= 300
    AND d.conversion_rate < m.conversion_rate * 0.9
),


-- 7.4 Device-level funnel priority signals
-- Uses the Medium device_funnel_priority table.

device_priority_signals AS (
  SELECT
    device_category AS place,
    'device' AS entity_type,
    'device_revenue_leak_priority' AS signal_type,

    LOWER(priority) AS severity,

    device_category,
    biggest_leak_step AS funnel_step,
    priority,

    sessions,
    transactions,
    revenue AS actual_revenue,
    total_lost_revenue AS lost_revenue,
    conversion_rate,
    biggest_leak_dropoff_rate AS dropoff_rate,
    NULL AS revenue_confidence,

    CONCAT(
      device_category,
      ' is a ',
      LOWER(priority),
      ' priority device. Total estimated lost revenue: ',
      CAST(ROUND(total_lost_revenue, 2) AS STRING),
      '. Biggest leak: ',
      IFNULL(biggest_leak_step, 'not enough data'),
      '.'
    ) AS issues,

    CASE
      WHEN biggest_leak_step LIKE '%Added to cart → Viewed cart%'
        THEN 'Review cart visibility, mini-cart behaviour, add-to-cart feedback, sticky cart buttons, and whether users clearly understand how to reach the cart.'

      WHEN biggest_leak_step LIKE '%Viewed cart → Started checkout%'
        THEN 'Review cart page clarity, checkout CTA visibility, delivery cost messaging, free-delivery threshold, and trust signals before checkout.'

      WHEN biggest_leak_step LIKE '%Started checkout → Added shipping info%'
        THEN 'Review checkout form friction, mobile usability, address fields, account/login interruptions, and delivery information visibility.'

      WHEN biggest_leak_step LIKE '%Added shipping info → Added payment info%'
        THEN 'Review delivery options, shipping cost surprises, delivery date expectations, and whether users see blockers before payment.'

      WHEN biggest_leak_step LIKE '%Added payment info → Purchased%'
        THEN 'Review payment errors, payment methods, 3DS friction, payment trust signals, and failed transaction behaviour.'

      ELSE 'Review the biggest device-specific funnel step and compare it with other devices to identify whether the issue is device-specific or site-wide.'
    END AS recommended_action

  FROM `YOUR_PROJECT.leakonic.device_funnel_priority`
  WHERE priority IN ('High', 'Medium')
),


-- 7.5 Device-specific funnel step drop-off signals
-- This replaces the Starter global funnel_signals logic.

device_funnel_signals AS (
  SELECT
    CONCAT(device_category, ': ', transition_label) AS place,
    'device_funnel_step' AS entity_type,
    'device_funnel_step_dropoff' AS signal_type,

    CASE
      WHEN lost_revenue >= 10000 AND dropoff_rate >= 0.5 THEN 'high'
      WHEN lost_revenue >= 3000 AND dropoff_rate >= 0.3 THEN 'medium'
      WHEN dropoff_rate >= 0.3 THEN 'low'
      ELSE 'low'
    END AS severity,

    device_category,
    transition_label AS funnel_step,
    NULL AS priority,

    from_sessions AS sessions,
    NULL AS transactions,
    NULL AS actual_revenue,
    lost_revenue,
    conversion_rate,
    dropoff_rate,
    revenue_confidence,

    CONCAT(
      device_category,
      ' users drop off between ',
      transition_label,
      '. Drop-off rate: ',
      CAST(ROUND(dropoff_rate * 100, 2) AS STRING),
      '%. Estimated lost revenue: ',
      CAST(ROUND(lost_revenue, 2) AS STRING),
      '. Revenue confidence: ',
      IFNULL(revenue_confidence, 'Unknown'),
      '.'
    ) AS issues,

    CASE
      WHEN from_step = 'add_to_cart' AND to_step = 'view_cart'
        THEN 'Check whether users clearly see that the product was added to cart, whether the cart icon is visible, and whether the next step to view cart is obvious.'

      WHEN from_step = 'view_cart' AND to_step = 'begin_checkout'
        THEN 'Check the cart page CTA, delivery cost visibility, minimum order or free-delivery messaging, trust signals, and whether the checkout button is prominent on this device.'

      WHEN from_step = 'begin_checkout' AND to_step = 'add_shipping_info'
        THEN 'Check checkout form usability, login/account friction, address field errors, mobile layout problems, and whether delivery expectations are clear.'

      WHEN from_step = 'add_shipping_info' AND to_step = 'add_payment_info'
        THEN 'Check shipping method clarity, delivery date availability, delivery cost surprises, and whether users face blockers before payment.'

      WHEN from_step = 'add_payment_info' AND to_step = 'purchase'
        THEN 'Check payment errors, available payment methods, card validation, 3DS issues, and final order confirmation friction.'

      ELSE 'Review this funnel step by device and compare it with other devices to identify whether the issue is technical, UX-related, or expectation-related.'
    END AS recommended_action

  FROM `YOUR_PROJECT.leakonic.funnel_transitions_by_device`
  WHERE from_sessions >= 50
    AND (
      dropoff_rate >= 0.3
      OR lost_revenue >= 3000
    )
),


-- 7.6 Single biggest device-step revenue leak
-- One headline signal for the Medium package.

biggest_device_step_leak AS (
  SELECT
    CONCAT(device_category, ': ', transition_label) AS place,
    'device_funnel_step' AS entity_type,
    'biggest_device_step_revenue_leak' AS signal_type,

    'high' AS severity,

    device_category,
    transition_label AS funnel_step,
    'High' AS priority,

    from_sessions AS sessions,
    NULL AS transactions,
    NULL AS actual_revenue,
    lost_revenue,
    conversion_rate,
    dropoff_rate,
    revenue_confidence,

    CONCAT(
      'The biggest detected device-level revenue leak is ',
      device_category,
      ' users at ',
      transition_label,
      '. Estimated lost revenue: ',
      CAST(ROUND(lost_revenue, 2) AS STRING),
      '. Drop-off rate: ',
      CAST(ROUND(dropoff_rate * 100, 2) AS STRING),
      '%.'
    ) AS issues,

    'Prioritise this device-step combination first because it has the largest estimated revenue impact in the funnel.' AS recommended_action

  FROM `YOUR_PROJECT.leakonic.funnel_transitions_by_device`
  WHERE lost_revenue > 0
  QUALIFY ROW_NUMBER() OVER (ORDER BY lost_revenue DESC) = 1
),


-- 7.7 High drop-off, but low business impact
-- Helps prevent users from chasing small segments only because the percentage looks bad.

high_dropoff_low_impact_signals AS (
  SELECT
    CONCAT(device_category, ': ', transition_label) AS place,
    'device_funnel_step' AS entity_type,
    'high_dropoff_low_impact' AS signal_type,

    'low' AS severity,

    device_category,
    transition_label AS funnel_step,
    'Low' AS priority,

    from_sessions AS sessions,
    NULL AS transactions,
    NULL AS actual_revenue,
    lost_revenue,
    conversion_rate,
    dropoff_rate,
    revenue_confidence,

    CONCAT(
      device_category,
      ' has a high drop-off rate at ',
      transition_label,
      ', but the estimated lost revenue is relatively low. Drop-off rate: ',
      CAST(ROUND(dropoff_rate * 100, 2) AS STRING),
      '%, estimated lost revenue: ',
      CAST(ROUND(lost_revenue, 2) AS STRING),
      '.'
    ) AS issues,

    'Do not prioritise this issue before higher-revenue leaks unless it affects a strategically important segment.' AS recommended_action

  FROM `YOUR_PROJECT.leakonic.funnel_transitions_by_device`
  WHERE from_sessions >= 50
    AND dropoff_rate >= 0.6
    AND lost_revenue < 3000
),


-- 7.8 Strong device funnel performance
-- Optional positive/neutral signal. Keeps the report from being only negative.

strong_device_funnel_steps AS (
  SELECT
    CONCAT(device_category, ': ', transition_label) AS place,
    'device_funnel_step' AS entity_type,
    'strong_device_funnel_step' AS signal_type,

    'low' AS severity,

    device_category,
    transition_label AS funnel_step,
    NULL AS priority,

    from_sessions AS sessions,
    NULL AS transactions,
    NULL AS actual_revenue,
    lost_revenue,
    conversion_rate,
    dropoff_rate,
    revenue_confidence,

    CONCAT(
      device_category,
      ' performs relatively well at ',
      transition_label,
      '. Drop-off rate is only ',
      CAST(ROUND(dropoff_rate * 100, 2) AS STRING),
      '%.'
    ) AS issues,

    'Use this step as a benchmark when reviewing weaker device-specific funnel steps.' AS recommended_action

  FROM `YOUR_PROJECT.leakonic.funnel_transitions_by_device`
  WHERE from_sessions >= 300
    AND dropoff_rate <= 0.2
)


SELECT
  place,
  entity_type,
  signal_type,
  severity,

  CASE
    WHEN severity = 'high' THEN 3
    WHEN severity = 'medium' THEN 2
    WHEN severity = 'low' THEN 1
    ELSE 0
  END AS severity_score,

  device_category,
  funnel_step,
  priority,

  sessions,
  transactions,
  actual_revenue,
  lost_revenue,
  conversion_rate,
  dropoff_rate,
  revenue_confidence,

  issues,
  recommended_action

FROM (
  SELECT * FROM landing_signals
  UNION ALL SELECT * FROM no_revenue_signals
  UNION ALL SELECT * FROM device_gap
  UNION ALL SELECT * FROM device_priority_signals
  UNION ALL SELECT * FROM device_funnel_signals
  UNION ALL SELECT * FROM biggest_device_step_leak
  UNION ALL SELECT * FROM high_dropoff_low_impact_signals
  UNION ALL SELECT * FROM strong_device_funnel_steps
)

ORDER BY
  severity_score DESC,
  lost_revenue DESC,
  sessions DESC;
