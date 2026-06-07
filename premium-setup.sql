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

  SELECT
    CAST(place AS STRING) AS place,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(signal_type AS STRING) AS signal_type,
    CAST(severity AS STRING) AS severity,
    CAST(device_category AS STRING) AS device_category,
    CAST(funnel_step AS STRING) AS funnel_step,
    CAST(priority AS STRING) AS priority,
    CAST(sessions AS INT64) AS sessions,
    CAST(transactions AS INT64) AS transactions,
    CAST(actual_revenue AS FLOAT64) AS actual_revenue,
    CAST(lost_revenue AS FLOAT64) AS lost_revenue,
    CAST(conversion_rate AS FLOAT64) AS conversion_rate,
    CAST(dropoff_rate AS FLOAT64) AS dropoff_rate,
    CAST(revenue_confidence AS STRING) AS revenue_confidence,
    CAST(issues AS STRING) AS issues,
    CAST(recommended_action AS STRING) AS recommended_action
  FROM landing_signals

  UNION ALL

  SELECT
    CAST(place AS STRING),
    CAST(entity_type AS STRING),
    CAST(signal_type AS STRING),
    CAST(severity AS STRING),
    CAST(device_category AS STRING),
    CAST(funnel_step AS STRING),
    CAST(priority AS STRING),
    CAST(sessions AS INT64),
    CAST(transactions AS INT64),
    CAST(actual_revenue AS FLOAT64),
    CAST(lost_revenue AS FLOAT64),
    CAST(conversion_rate AS FLOAT64),
    CAST(dropoff_rate AS FLOAT64),
    CAST(revenue_confidence AS STRING),
    CAST(issues AS STRING),
    CAST(recommended_action AS STRING)
  FROM no_revenue_signals

  UNION ALL

  SELECT
    CAST(place AS STRING),
    CAST(entity_type AS STRING),
    CAST(signal_type AS STRING),
    CAST(severity AS STRING),
    CAST(device_category AS STRING),
    CAST(funnel_step AS STRING),
    CAST(priority AS STRING),
    CAST(sessions AS INT64),
    CAST(transactions AS INT64),
    CAST(actual_revenue AS FLOAT64),
    CAST(lost_revenue AS FLOAT64),
    CAST(conversion_rate AS FLOAT64),
    CAST(dropoff_rate AS FLOAT64),
    CAST(revenue_confidence AS STRING),
    CAST(issues AS STRING),
    CAST(recommended_action AS STRING)
  FROM device_gap

  UNION ALL

  SELECT
    CAST(place AS STRING),
    CAST(entity_type AS STRING),
    CAST(signal_type AS STRING),
    CAST(severity AS STRING),
    CAST(device_category AS STRING),
    CAST(funnel_step AS STRING),
    CAST(priority AS STRING),
    CAST(sessions AS INT64),
    CAST(transactions AS INT64),
    CAST(actual_revenue AS FLOAT64),
    CAST(lost_revenue AS FLOAT64),
    CAST(conversion_rate AS FLOAT64),
    CAST(dropoff_rate AS FLOAT64),
    CAST(revenue_confidence AS STRING),
    CAST(issues AS STRING),
    CAST(recommended_action AS STRING)
  FROM device_priority_signals

  UNION ALL

  SELECT
    CAST(place AS STRING),
    CAST(entity_type AS STRING),
    CAST(signal_type AS STRING),
    CAST(severity AS STRING),
    CAST(device_category AS STRING),
    CAST(funnel_step AS STRING),
    CAST(priority AS STRING),
    CAST(sessions AS INT64),
    CAST(transactions AS INT64),
    CAST(actual_revenue AS FLOAT64),
    CAST(lost_revenue AS FLOAT64),
    CAST(conversion_rate AS FLOAT64),
    CAST(dropoff_rate AS FLOAT64),
    CAST(revenue_confidence AS STRING),
    CAST(issues AS STRING),
    CAST(recommended_action AS STRING)
  FROM device_funnel_signals

  UNION ALL

  SELECT
    CAST(place AS STRING),
    CAST(entity_type AS STRING),
    CAST(signal_type AS STRING),
    CAST(severity AS STRING),
    CAST(device_category AS STRING),
    CAST(funnel_step AS STRING),
    CAST(priority AS STRING),
    CAST(sessions AS INT64),
    CAST(transactions AS INT64),
    CAST(actual_revenue AS FLOAT64),
    CAST(lost_revenue AS FLOAT64),
    CAST(conversion_rate AS FLOAT64),
    CAST(dropoff_rate AS FLOAT64),
    CAST(revenue_confidence AS STRING),
    CAST(issues AS STRING),
    CAST(recommended_action AS STRING)
  FROM biggest_device_step_leak

  UNION ALL

  SELECT
    CAST(place AS STRING),
    CAST(entity_type AS STRING),
    CAST(signal_type AS STRING),
    CAST(severity AS STRING),
    CAST(device_category AS STRING),
    CAST(funnel_step AS STRING),
    CAST(priority AS STRING),
    CAST(sessions AS INT64),
    CAST(transactions AS INT64),
    CAST(actual_revenue AS FLOAT64),
    CAST(lost_revenue AS FLOAT64),
    CAST(conversion_rate AS FLOAT64),
    CAST(dropoff_rate AS FLOAT64),
    CAST(revenue_confidence AS STRING),
    CAST(issues AS STRING),
    CAST(recommended_action AS STRING)
  FROM high_dropoff_low_impact_signals

  UNION ALL

  SELECT
    CAST(place AS STRING),
    CAST(entity_type AS STRING),
    CAST(signal_type AS STRING),
    CAST(severity AS STRING),
    CAST(device_category AS STRING),
    CAST(funnel_step AS STRING),
    CAST(priority AS STRING),
    CAST(sessions AS INT64),
    CAST(transactions AS INT64),
    CAST(actual_revenue AS FLOAT64),
    CAST(lost_revenue AS FLOAT64),
    CAST(conversion_rate AS FLOAT64),
    CAST(dropoff_rate AS FLOAT64),
    CAST(revenue_confidence AS STRING),
    CAST(issues AS STRING),
    CAST(recommended_action AS STRING)
  FROM strong_device_funnel_steps
)

ORDER BY
  severity_score DESC,
  lost_revenue DESC,
  sessions DESC;


-- 8. Product Analysis - Medium Package
-- Creates issue-focused product analysis tables:
-- 8A. product_session_funnel
-- 8B. product_performance
-- 8C. product_view_to_cart_issues
-- 8D. product_cart_to_purchase_issues
-- 8E. category_view_to_purchase_issues
-- 8F. category_cart_to_purchase_issues
-- 8G. top_sellers_with_lost_revenue


-- 8A. Product session funnel
-- One row per product per session.
-- This is the base table for all product issue analysis.

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.product_session_funnel` AS

WITH item_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,

    CONCAT(
      user_pseudo_id,
      '-',
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,

    user_pseudo_id,

    CASE
      WHEN LOWER(device.category) IN ('desktop', 'mobile', 'tablet')
        THEN LOWER(device.category)
      ELSE 'other'
    END AS device_category,

    event_name,
    event_timestamp,

    item.item_id,
    item.item_name,

    COALESCE(
      item.item_category,
      item.item_category2,
      item.item_category3,
      'Unknown'
    ) AS item_category,

    item.price AS item_price,
    item.quantity AS item_quantity,

    COALESCE(
      item.item_revenue,
      item.price * item.quantity,
      item.price,
      0
    ) AS item_value,

    ecommerce.transaction_id AS transaction_id

  FROM `YOUR_PROJECT.YOUR_GA4_DATASET.events_*`,
  UNNEST(items) AS item

  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
    AND event_name IN (
      'view_item',
      'add_to_cart',
      'purchase'
    )
    AND item.item_id IS NOT NULL
),

product_session AS (
  SELECT
    session_id,
    user_pseudo_id,
    ANY_VALUE(device_category) AS device_category,

    item_id,
    ANY_VALUE(item_name) AS item_name,
    ANY_VALUE(item_category) AS item_category,

    MAX(IF(event_name = 'view_item', 1, 0)) AS viewed_product,
    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS added_to_cart,
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchased_product,

    COUNTIF(event_name = 'view_item') AS view_item_events,
    COUNTIF(event_name = 'add_to_cart') AS add_to_cart_events,
    COUNTIF(event_name = 'purchase') AS purchase_events,

    SUM(IF(event_name = 'add_to_cart', IFNULL(item_value, 0), 0)) AS add_to_cart_value,
    SUM(IF(event_name = 'purchase', IFNULL(item_value, 0), 0)) AS purchase_revenue,

    COUNT(DISTINCT IF(event_name = 'purchase', transaction_id, NULL)) AS transactions

  FROM item_events
  WHERE session_id IS NOT NULL
  GROUP BY
    session_id,
    user_pseudo_id,
    item_id
)

SELECT
  *,

  CASE
    WHEN added_to_cart = 1
      AND purchased_product = 0
      THEN add_to_cart_value
    ELSE 0
  END AS estimated_lost_revenue_after_cart

FROM product_session;



-- 8B. Product performance
-- Main reusable product-level table.

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.product_performance` AS

WITH product_metrics AS (
  SELECT
    item_id,
    ANY_VALUE(item_name) AS item_name,
    ANY_VALUE(item_category) AS item_category,

    COUNT(DISTINCT IF(viewed_product = 1, session_id, NULL)) AS view_sessions,
    COUNT(DISTINCT IF(added_to_cart = 1, session_id, NULL)) AS add_to_cart_sessions,
    COUNT(DISTINCT IF(purchased_product = 1, session_id, NULL)) AS purchase_sessions,

    SUM(view_item_events) AS view_item_events,
    SUM(add_to_cart_events) AS add_to_cart_events,
    SUM(purchase_events) AS purchase_events,

    SUM(add_to_cart_value) AS add_to_cart_value,
    SUM(purchase_revenue) AS purchase_revenue,
    SUM(estimated_lost_revenue_after_cart) AS estimated_lost_revenue,

    SUM(transactions) AS transactions

  FROM `YOUR_PROJECT.leakonic.product_session_funnel`
  GROUP BY item_id
)

SELECT
  item_id,
  item_name,
  item_category,

  view_sessions,
  add_to_cart_sessions,
  purchase_sessions,

  view_item_events,
  add_to_cart_events,
  purchase_events,

  add_to_cart_value,
  purchase_revenue,
  estimated_lost_revenue,

  transactions,

  SAFE_DIVIDE(add_to_cart_sessions, view_sessions) AS view_to_cart_rate,
  SAFE_DIVIDE(purchase_sessions, view_sessions) AS view_to_purchase_rate,
  SAFE_DIVIDE(purchase_sessions, add_to_cart_sessions) AS cart_to_purchase_rate,

  SAFE_DIVIDE(estimated_lost_revenue, purchase_revenue) AS lost_revenue_vs_actual_revenue,
  SAFE_DIVIDE(estimated_lost_revenue, add_to_cart_sessions) AS lost_revenue_per_cart_session

FROM product_metrics;



-- 8C. Products with many views but few add-to-cart events
-- Product page / offer / price / trust issue.

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.product_view_to_cart_issues` AS

WITH averages AS (
  SELECT
    AVG(view_to_cart_rate) AS avg_view_to_cart_rate
  FROM `YOUR_PROJECT.leakonic.product_performance`
  WHERE view_sessions >= 20
    AND view_to_cart_rate IS NOT NULL
)

SELECT
  p.item_id,
  p.item_name,
  p.item_category,

  p.view_sessions,
  p.add_to_cart_sessions,
  p.purchase_sessions,

  p.view_to_cart_rate,
  p.view_to_purchase_rate,
  p.cart_to_purchase_rate,

  p.purchase_revenue,
  p.estimated_lost_revenue,

  a.avg_view_to_cart_rate,
  p.view_to_cart_rate - a.avg_view_to_cart_rate AS gap_vs_average_view_to_cart_rate,

  CASE
    WHEN p.view_sessions >= 500
      AND p.view_to_cart_rate < a.avg_view_to_cart_rate * 0.5
      THEN 'High'

    WHEN p.view_sessions >= 100
      AND p.view_to_cart_rate < a.avg_view_to_cart_rate * 0.7
      THEN 'Medium'

    ELSE 'Low'
  END AS priority,

  'High views, low add-to-cart' AS issue_type,

  CONCAT(
    'This product receives product views but has a weak add-to-cart rate. Views: ',
    CAST(p.view_sessions AS STRING),
    ', add-to-cart rate: ',
    CAST(ROUND(p.view_to_cart_rate * 100, 2) AS STRING),
    '%.'
  ) AS issue,

  'Review product image, title, price visibility, description, freshness or delivery reassurance, product availability, and add-to-cart button visibility.' AS recommended_action

FROM `YOUR_PROJECT.leakonic.product_performance` p
CROSS JOIN averages a

WHERE p.view_sessions >= 100
  AND p.view_to_cart_rate < a.avg_view_to_cart_rate * 0.7

ORDER BY
  priority DESC,
  view_sessions DESC;



-- 8D. Products with many add-to-carts but few purchases
-- Cart / checkout / stock / delivery expectation issue.

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.product_cart_to_purchase_issues` AS

WITH averages AS (
  SELECT
    AVG(cart_to_purchase_rate) AS avg_cart_to_purchase_rate
  FROM `YOUR_PROJECT.leakonic.product_performance`
  WHERE add_to_cart_sessions >= 10
    AND cart_to_purchase_rate IS NOT NULL
)

SELECT
  p.item_id,
  p.item_name,
  p.item_category,

  p.view_sessions,
  p.add_to_cart_sessions,
  p.purchase_sessions,

  p.view_to_cart_rate,
  p.view_to_purchase_rate,
  p.cart_to_purchase_rate,

  p.add_to_cart_value,
  p.purchase_revenue,
  p.estimated_lost_revenue,
  p.lost_revenue_vs_actual_revenue,

  a.avg_cart_to_purchase_rate,
  p.cart_to_purchase_rate - a.avg_cart_to_purchase_rate AS gap_vs_average_cart_to_purchase_rate,

  CASE
    WHEN p.add_to_cart_sessions >= 100
      AND p.estimated_lost_revenue >= 5000
      AND p.cart_to_purchase_rate < a.avg_cart_to_purchase_rate * 0.7
      THEN 'High'

    WHEN p.add_to_cart_sessions >= 30
      AND p.estimated_lost_revenue >= 1000
      AND p.cart_to_purchase_rate < a.avg_cart_to_purchase_rate * 0.85
      THEN 'Medium'

    ELSE 'Low'
  END AS priority,

  'High add-to-cart, low purchase' AS issue_type,

  CONCAT(
    'Users add this product to cart, but many sessions do not end with purchase. Add-to-cart sessions: ',
    CAST(p.add_to_cart_sessions AS STRING),
    ', cart-to-purchase rate: ',
    CAST(ROUND(p.cart_to_purchase_rate * 100, 2) AS STRING),
    '%, estimated lost revenue: ',
    CAST(ROUND(p.estimated_lost_revenue, 2) AS STRING),
    '.'
  ) AS issue,

  'Check stock availability, delivery restrictions, shipping cost expectations, minimum order value, checkout friction, and whether the product remains available later in the journey.' AS recommended_action

FROM `YOUR_PROJECT.leakonic.product_performance` p
CROSS JOIN averages a

WHERE p.add_to_cart_sessions >= 30
  AND p.cart_to_purchase_rate < a.avg_cart_to_purchase_rate * 0.85
  AND p.estimated_lost_revenue > 0

ORDER BY
  estimated_lost_revenue DESC;



-- 8E. Categories with many views but few purchases
-- Category-level high interest, weak purchase conversion.

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.category_view_to_purchase_issues` AS

WITH category_metrics AS (
  SELECT
    item_category,

    COUNT(DISTINCT IF(viewed_product = 1, session_id, NULL)) AS view_sessions,
    COUNT(DISTINCT IF(added_to_cart = 1, session_id, NULL)) AS add_to_cart_sessions,
    COUNT(DISTINCT IF(purchased_product = 1, session_id, NULL)) AS purchase_sessions,

    SUM(add_to_cart_value) AS add_to_cart_value,
    SUM(purchase_revenue) AS purchase_revenue,
    SUM(estimated_lost_revenue_after_cart) AS estimated_lost_revenue

  FROM `YOUR_PROJECT.leakonic.product_session_funnel`
  GROUP BY item_category
),

averages AS (
  SELECT
    AVG(SAFE_DIVIDE(purchase_sessions, view_sessions)) AS avg_category_view_to_purchase_rate
  FROM category_metrics
  WHERE view_sessions >= 100
)

SELECT
  c.item_category,

  c.view_sessions,
  c.add_to_cart_sessions,
  c.purchase_sessions,

  SAFE_DIVIDE(c.add_to_cart_sessions, c.view_sessions) AS view_to_cart_rate,
  SAFE_DIVIDE(c.purchase_sessions, c.view_sessions) AS view_to_purchase_rate,
  SAFE_DIVIDE(c.purchase_sessions, c.add_to_cart_sessions) AS cart_to_purchase_rate,

  c.add_to_cart_value,
  c.purchase_revenue,
  c.estimated_lost_revenue,

  a.avg_category_view_to_purchase_rate,

  SAFE_DIVIDE(c.purchase_sessions, c.view_sessions) - a.avg_category_view_to_purchase_rate AS gap_vs_average_view_to_purchase_rate,

  CASE
    WHEN c.view_sessions >= 1000
      AND SAFE_DIVIDE(c.purchase_sessions, c.view_sessions) < a.avg_category_view_to_purchase_rate * 0.5
      THEN 'High'

    WHEN c.view_sessions >= 300
      AND SAFE_DIVIDE(c.purchase_sessions, c.view_sessions) < a.avg_category_view_to_purchase_rate * 0.7
      THEN 'Medium'

    ELSE 'Low'
  END AS priority,

  'High category interest, low purchase' AS issue_type,

  CONCAT(
    'This category receives product views but has weak purchase conversion. View sessions: ',
    CAST(c.view_sessions AS STRING),
    ', view-to-purchase rate: ',
    CAST(ROUND(SAFE_DIVIDE(c.purchase_sessions, c.view_sessions) * 100, 2) AS STRING),
    '%.'
  ) AS issue,

  'Review category merchandising, product relevance, price competitiveness, delivery expectations, trust messaging, and whether products in this category need clearer reassurance.' AS recommended_action

FROM category_metrics c
CROSS JOIN averages a

WHERE c.view_sessions >= 300
  AND SAFE_DIVIDE(c.purchase_sessions, c.view_sessions) < a.avg_category_view_to_purchase_rate * 0.7

ORDER BY
  view_sessions DESC;



-- 8F. Categories with many add-to-carts but few purchases
-- Category-level high cart intent, weak purchase completion.

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.category_cart_to_purchase_issues` AS

WITH category_metrics AS (
  SELECT
    item_category,

    COUNT(DISTINCT IF(viewed_product = 1, session_id, NULL)) AS view_sessions,
    COUNT(DISTINCT IF(added_to_cart = 1, session_id, NULL)) AS add_to_cart_sessions,
    COUNT(DISTINCT IF(purchased_product = 1, session_id, NULL)) AS purchase_sessions,

    SUM(add_to_cart_value) AS add_to_cart_value,
    SUM(purchase_revenue) AS purchase_revenue,
    SUM(estimated_lost_revenue_after_cart) AS estimated_lost_revenue

  FROM `YOUR_PROJECT.leakonic.product_session_funnel`
  GROUP BY item_category
),

averages AS (
  SELECT
    AVG(SAFE_DIVIDE(purchase_sessions, add_to_cart_sessions)) AS avg_category_cart_to_purchase_rate
  FROM category_metrics
  WHERE add_to_cart_sessions >= 30
)

SELECT
  c.item_category,

  c.view_sessions,
  c.add_to_cart_sessions,
  c.purchase_sessions,

  SAFE_DIVIDE(c.add_to_cart_sessions, c.view_sessions) AS view_to_cart_rate,
  SAFE_DIVIDE(c.purchase_sessions, c.view_sessions) AS view_to_purchase_rate,
  SAFE_DIVIDE(c.purchase_sessions, c.add_to_cart_sessions) AS cart_to_purchase_rate,

  c.add_to_cart_value,
  c.purchase_revenue,
  c.estimated_lost_revenue,

  a.avg_category_cart_to_purchase_rate,

  SAFE_DIVIDE(c.purchase_sessions, c.add_to_cart_sessions) - a.avg_category_cart_to_purchase_rate AS gap_vs_average_cart_to_purchase_rate,

  CASE
    WHEN c.add_to_cart_sessions >= 300
      AND c.estimated_lost_revenue >= 10000
      AND SAFE_DIVIDE(c.purchase_sessions, c.add_to_cart_sessions) < a.avg_category_cart_to_purchase_rate * 0.7
      THEN 'High'

    WHEN c.add_to_cart_sessions >= 100
      AND c.estimated_lost_revenue >= 3000
      AND SAFE_DIVIDE(c.purchase_sessions, c.add_to_cart_sessions) < a.avg_category_cart_to_purchase_rate * 0.85
      THEN 'Medium'

    ELSE 'Low'
  END AS priority,

  'High category cart intent, low purchase completion' AS issue_type,

  CONCAT(
    'Products in this category are added to cart, but many sessions do not complete purchase. Add-to-cart sessions: ',
    CAST(c.add_to_cart_sessions AS STRING),
    ', cart-to-purchase rate: ',
    CAST(ROUND(SAFE_DIVIDE(c.purchase_sessions, c.add_to_cart_sessions) * 100, 2) AS STRING),
    '%, estimated lost revenue: ',
    CAST(ROUND(c.estimated_lost_revenue, 2) AS STRING),
    '.'
  ) AS issue,

  'Check category-specific delivery conditions, shipping cost expectations, stock availability, checkout friction, minimum order value, and whether customers lose confidence before purchase.' AS recommended_action

FROM category_metrics c
CROSS JOIN averages a

WHERE c.add_to_cart_sessions >= 100
  AND SAFE_DIVIDE(c.purchase_sessions, c.add_to_cart_sessions) < a.avg_category_cart_to_purchase_rate * 0.85
  AND c.estimated_lost_revenue > 0

ORDER BY
  estimated_lost_revenue DESC;



-- 8G. Top sellers with estimated lost revenue
-- Strong sellers that still leak significant potential revenue.

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.top_sellers_with_lost_revenue` AS

WITH ranked_products AS (
  SELECT
    *,

    RANK() OVER (
      ORDER BY purchase_revenue DESC
    ) AS revenue_rank,

    SAFE_DIVIDE(
      estimated_lost_revenue,
      purchase_revenue
    ) AS lost_revenue_vs_actual_revenue_recalculated

  FROM `YOUR_PROJECT.leakonic.product_performance`
  WHERE purchase_revenue > 0
)

SELECT
  item_id,
  item_name,
  item_category,

  revenue_rank,

  view_sessions,
  add_to_cart_sessions,
  purchase_sessions,

  view_to_cart_rate,
  view_to_purchase_rate,
  cart_to_purchase_rate,

  purchase_revenue,
  estimated_lost_revenue,
  lost_revenue_vs_actual_revenue_recalculated AS lost_revenue_vs_actual_revenue,

  CASE
    WHEN revenue_rank <= 20
      AND estimated_lost_revenue >= 5000
      THEN 'High'

    WHEN revenue_rank <= 50
      AND estimated_lost_revenue >= 1000
      THEN 'Medium'

    ELSE 'Low'
  END AS priority,

  'Strong seller with hidden revenue leakage' AS issue_type,

  CONCAT(
    'This product is already a strong seller, but it also has meaningful estimated lost revenue. Revenue rank: ',
    CAST(revenue_rank AS STRING),
    ', purchase revenue: ',
    CAST(ROUND(purchase_revenue, 2) AS STRING),
    ', estimated lost revenue: ',
    CAST(ROUND(estimated_lost_revenue, 2) AS STRING),
    '.'
  ) AS issue,

  'Prioritise this product because it already has proven demand. Review product page clarity, cart behaviour, delivery messaging, stock availability, and checkout friction.' AS recommended_action

FROM ranked_products

WHERE revenue_rank <= 50
  AND estimated_lost_revenue > 0

ORDER BY
  estimated_lost_revenue DESC;

-- Premium SEO Package — Google Search Console BigQuery Transformations
-- Creates all 5 SEO report sections:
-- 1) SEO Overview
-- 2) Cannibalization
-- 3) CTR Issues
-- 4) Clicks / Traffic Issues
-- 5) Keyword Position Issues
--
-- Before running:
-- 1. Replace `YOUR_PROJECT.searchconsole` with your GSC BigQuery export project.dataset.
-- 2. Make sure the output dataset already exists.
CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.leakonic`;

DECLARE period_days INT64 DEFAULT 28;
DECLARE current_start_offset INT64 DEFAULT 27;
DECLARE previous_start_offset INT64 DEFAULT 55;

DECLARE min_overview_impressions INT64 DEFAULT 50;

DECLARE min_cannibalization_query_impressions INT64 DEFAULT 100;
DECLARE min_cannibalization_url_impressions INT64 DEFAULT 10;
DECLARE max_urls_per_keyword INT64 DEFAULT 10;

DECLARE min_ctr_current_impressions INT64 DEFAULT 100;
DECLARE min_ctr_benchmark_impressions INT64 DEFAULT 500;
DECLARE min_ctr_gap FLOAT64 DEFAULT 0.005;

DECLARE min_traffic_impressions INT64 DEFAULT 50;
DECLARE min_traffic_lost_clicks INT64 DEFAULT 1;

DECLARE min_position_impressions INT64 DEFAULT 50;
DECLARE min_position_drop FLOAT64 DEFAULT 1.0;
DECLARE min_position_lost_clicks INT64 DEFAULT 0;


-- ============================================================
-- PART 1. SEO OVERVIEW
-- ============================================================

-- 1.1 Daily organic traffic trend

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_overview_daily`
PARTITION BY data_date
AS
SELECT
  data_date,

  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,

  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,

  SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) + 1 AS avg_position

FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
GROUP BY data_date;


-- 1.2 Traffic split charts: device, country, search type

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_overview_splits_daily`
PARTITION BY data_date
CLUSTER BY split_type, split_value
AS

SELECT
  data_date,
  'device' AS split_type,
  COALESCE(device, 'UNKNOWN') AS split_value,

  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
  SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) + 1 AS avg_position

FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
GROUP BY data_date, split_type, split_value

UNION ALL

SELECT
  data_date,
  'country' AS split_type,
  COALESCE(country, 'UNKNOWN') AS split_value,

  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
  SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) + 1 AS avg_position

FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
GROUP BY data_date, split_type, split_value

UNION ALL

SELECT
  data_date,
  'search_type' AS split_type,
  COALESCE(search_type, 'UNKNOWN') AS split_value,

  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
  SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) + 1 AS avg_position

FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
GROUP BY data_date, split_type, split_value;


-- 1.3 Top Organic Leaks table

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_overview_top_leaks`
PARTITION BY snapshot_date
CLUSTER BY issue_type
AS

WITH latest_date AS (
  SELECT
    MIN(max_data_date) AS snapshot_date
  FROM (
    SELECT MAX(data_date) AS max_data_date
    FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`

    UNION ALL

    SELECT MAX(data_date) AS max_data_date
    FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  )
),

periodized AS (
  SELECT
    l.snapshot_date,
    url,
    query,

    CASE
      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                         AND l.snapshot_date
        THEN 'current'

      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                         AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
        THEN 'previous'
    END AS period,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(sum_position) AS sum_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date
    AND query IS NOT NULL
    AND url IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    url,
    query,
    period
),

pivoted AS (
  SELECT
    snapshot_date,
    url,
    query,

    SUM(IF(period = 'current', clicks, 0)) AS current_clicks,
    SUM(IF(period = 'previous', clicks, 0)) AS previous_clicks,

    SUM(IF(period = 'current', impressions, 0)) AS current_impressions,
    SUM(IF(period = 'previous', impressions, 0)) AS previous_impressions,

    SUM(IF(period = 'current', sum_position, 0)) AS current_sum_position,
    SUM(IF(period = 'previous', sum_position, 0)) AS previous_sum_position

  FROM periodized
  WHERE period IS NOT NULL
  GROUP BY
    snapshot_date,
    url,
    query
),

calculated AS (
  SELECT
    snapshot_date,
    url,
    query,

    current_clicks,
    previous_clicks,
    current_clicks - previous_clicks AS click_change,
    GREATEST(0, previous_clicks - current_clicks) AS lost_clicks,

    current_impressions,
    previous_impressions,
    current_impressions - previous_impressions AS impressions_change,

    SAFE_DIVIDE(current_clicks, current_impressions) AS current_ctr,
    SAFE_DIVIDE(previous_clicks, previous_impressions) AS previous_ctr,

    SAFE_DIVIDE(current_clicks, current_impressions)
      - SAFE_DIVIDE(previous_clicks, previous_impressions) AS ctr_change,

    SAFE_DIVIDE(current_sum_position, current_impressions) + 1 AS current_avg_position,
    SAFE_DIVIDE(previous_sum_position, previous_impressions) + 1 AS previous_avg_position,

    SAFE_DIVIDE(current_sum_position, current_impressions) + 1
      - (SAFE_DIVIDE(previous_sum_position, previous_impressions) + 1) AS position_change

  FROM pivoted
  WHERE current_impressions >= min_overview_impressions
     OR previous_impressions >= min_overview_impressions
),

classified AS (
  SELECT
    *,

    CASE
      WHEN lost_clicks = 0 THEN 'No click loss'

      WHEN position_change >= 1
        THEN 'Position issue'

      WHEN ctr_change <= -0.005
        AND current_impressions >= previous_impressions * 0.8
        THEN 'CTR issue'

      WHEN impressions_change < 0
        THEN 'Visibility issue'

      ELSE 'Traffic issue'
    END AS issue_type

  FROM calculated
)

SELECT
  *
FROM classified
WHERE lost_clicks > 0;


-- 1.4 SEO Overview scorecards

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_overview_scorecards`
PARTITION BY snapshot_date
AS

WITH latest_date AS (
  SELECT
    MIN(max_data_date) AS snapshot_date
  FROM (
    SELECT MAX(data_date) AS max_data_date
    FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`

    UNION ALL

    SELECT MAX(data_date) AS max_data_date
    FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  )
),

site_periods AS (
  SELECT
    l.snapshot_date,

    CASE
      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                         AND l.snapshot_date
        THEN 'current'

      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                         AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
        THEN 'previous'
    END AS period,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(sum_top_position) AS sum_top_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date

  GROUP BY
    l.snapshot_date,
    period
),

site_pivoted AS (
  SELECT
    snapshot_date,

    SUM(IF(period = 'current', clicks, 0)) AS current_clicks,
    SUM(IF(period = 'previous', clicks, 0)) AS previous_clicks,

    SUM(IF(period = 'current', impressions, 0)) AS current_impressions,
    SUM(IF(period = 'previous', impressions, 0)) AS previous_impressions,

    SUM(IF(period = 'current', sum_top_position, 0)) AS current_sum_top_position,
    SUM(IF(period = 'previous', sum_top_position, 0)) AS previous_sum_top_position

  FROM site_periods
  WHERE period IS NOT NULL
  GROUP BY snapshot_date
),

issue_counts AS (
  SELECT
    snapshot_date,
    COUNT(DISTINCT url) AS urls_with_issues,
    COUNT(DISTINCT query) AS queries_with_issues,
    SUM(lost_clicks) AS issue_level_lost_clicks
  FROM `YOUR_PROJECT.leakonic.seo_overview_top_leaks`
  GROUP BY snapshot_date
)

SELECT
  p.snapshot_date,

  period_days AS period_days,

  p.current_clicks AS organic_clicks,
  p.previous_clicks AS previous_organic_clicks,

  p.current_clicks - p.previous_clicks AS click_change,

  GREATEST(0, p.previous_clicks - p.current_clicks) AS estimated_lost_clicks,

  p.current_impressions AS organic_impressions,
  p.previous_impressions AS previous_organic_impressions,

  SAFE_DIVIDE(p.current_clicks, p.current_impressions) AS ctr,

  SAFE_DIVIDE(p.current_sum_top_position, p.current_impressions) + 1 AS avg_position,

  IFNULL(i.urls_with_issues, 0) AS urls_with_issues,
  IFNULL(i.queries_with_issues, 0) AS queries_with_issues

FROM site_pivoted p
LEFT JOIN issue_counts i
  USING (snapshot_date);


-- ============================================================
-- PART 2. CANNIBALIZATION
-- ============================================================

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_cannibalization`
PARTITION BY snapshot_date
CLUSTER BY url_count, dominant_url_changed
AS

WITH latest_date AS (
  SELECT
    MIN(max_data_date) AS snapshot_date
  FROM (
    SELECT MAX(data_date) AS max_data_date
    FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`

    UNION ALL

    SELECT MAX(data_date) AS max_data_date
    FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  )
),

url_periodized AS (
  SELECT
    l.snapshot_date,
    query,
    url,

    CASE
      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                         AND l.snapshot_date
        THEN 'current'

      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                         AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
        THEN 'previous'
    END AS period,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(sum_position) AS sum_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date
    AND query IS NOT NULL
    AND url IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    query,
    url,
    period
),

url_pivoted AS (
  SELECT
    snapshot_date,
    query,
    url,

    SUM(IF(period = 'current', clicks, 0)) AS current_url_clicks,
    SUM(IF(period = 'previous', clicks, 0)) AS previous_url_clicks,

    SUM(IF(period = 'current', impressions, 0)) AS current_url_impressions,
    SUM(IF(period = 'previous', impressions, 0)) AS previous_url_impressions,

    SAFE_DIVIDE(
      SUM(IF(period = 'current', sum_position, 0)),
      SUM(IF(period = 'current', impressions, 0))
    ) + 1 AS current_url_avg_position,

    SAFE_DIVIDE(
      SUM(IF(period = 'previous', sum_position, 0)),
      SUM(IF(period = 'previous', impressions, 0))
    ) + 1 AS previous_url_avg_position

  FROM url_periodized
  WHERE period IS NOT NULL
  GROUP BY
    snapshot_date,
    query,
    url
),

current_url_candidates AS (
  SELECT *
  FROM url_pivoted
  WHERE current_url_impressions >= min_cannibalization_url_impressions
     OR current_url_clicks > 0
),

previous_url_candidates AS (
  SELECT *
  FROM url_pivoted
  WHERE previous_url_impressions >= min_cannibalization_url_impressions
     OR previous_url_clicks > 0
),

ranked_current_urls AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY snapshot_date, query
      ORDER BY
        current_url_clicks DESC,
        current_url_impressions DESC,
        current_url_avg_position ASC,
        url ASC
    ) AS current_url_rank
  FROM current_url_candidates
),

ranked_previous_urls AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY snapshot_date, query
      ORDER BY
        previous_url_clicks DESC,
        previous_url_impressions DESC,
        previous_url_avg_position ASC,
        url ASC
    ) AS previous_url_rank
  FROM previous_url_candidates
),

current_url_summary AS (
  SELECT
    snapshot_date,
    query,

    COUNT(DISTINCT url) AS url_count,

    SUM(current_url_clicks) AS current_url_total_clicks,
    SUM(current_url_impressions) AS current_url_total_impressions,

    MAX(IF(current_url_rank = 1, url, NULL)) AS current_dominant_url,
    MAX(IF(current_url_rank = 1, current_url_clicks, NULL)) AS current_dominant_url_clicks,
    MAX(IF(current_url_rank = 1, current_url_impressions, NULL)) AS current_dominant_url_impressions,
    MAX(IF(current_url_rank = 1, current_url_avg_position, NULL)) AS current_dominant_url_avg_position,

    STRING_AGG(
      IF(
        current_url_rank <= max_urls_per_keyword,
        CONCAT(
          url,
          ' — ',
          CAST(current_url_clicks AS STRING),
          ' clicks, ',
          CAST(current_url_impressions AS STRING),
          ' impressions, pos. ',
          COALESCE(CAST(ROUND(current_url_avg_position, 1) AS STRING), 'n/a')
        ),
        NULL
      ),
      '\n'
      ORDER BY current_url_rank
    ) AS ranked_urls

  FROM ranked_current_urls
  GROUP BY
    snapshot_date,
    query
),

previous_dominant_url AS (
  SELECT
    snapshot_date,
    query,
    url AS previous_dominant_url,
    previous_url_clicks AS previous_dominant_url_clicks,
    previous_url_impressions AS previous_dominant_url_impressions,
    previous_url_avg_position AS previous_dominant_url_avg_position

  FROM ranked_previous_urls
  WHERE previous_url_rank = 1
),

query_periodized AS (
  SELECT
    l.snapshot_date,
    query,

    CASE
      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                         AND l.snapshot_date
        THEN 'current'

      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                         AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
        THEN 'previous'
    END AS period,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(sum_top_position) AS sum_top_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date
    AND query IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    query,
    period
),

query_pivoted AS (
  SELECT
    snapshot_date,
    query,

    SUM(IF(period = 'current', clicks, 0)) AS current_clicks,
    SUM(IF(period = 'previous', clicks, 0)) AS previous_clicks,

    SUM(IF(period = 'current', impressions, 0)) AS current_impressions,
    SUM(IF(period = 'previous', impressions, 0)) AS previous_impressions,

    SAFE_DIVIDE(
      SUM(IF(period = 'current', sum_top_position, 0)),
      SUM(IF(period = 'current', impressions, 0))
    ) + 1 AS current_avg_position,

    SAFE_DIVIDE(
      SUM(IF(period = 'previous', sum_top_position, 0)),
      SUM(IF(period = 'previous', impressions, 0))
    ) + 1 AS previous_avg_position

  FROM query_periodized
  WHERE period IS NOT NULL
  GROUP BY
    snapshot_date,
    query
)

SELECT
  c.snapshot_date,

  c.query AS keyword,

  c.ranked_urls,
  c.url_count,

  p.previous_dominant_url,
  c.current_dominant_url,

  CASE
    WHEN p.previous_dominant_url IS NULL THEN NULL
    WHEN p.previous_dominant_url != c.current_dominant_url THEN TRUE
    ELSE FALSE
  END AS dominant_url_changed,

  SAFE_DIVIDE(
    c.current_dominant_url_clicks,
    c.current_url_total_clicks
  ) AS main_url_click_share,

  q.previous_clicks,
  q.current_clicks,

  q.current_clicks - q.previous_clicks AS click_change,

  GREATEST(0, q.previous_clicks - q.current_clicks) AS lost_clicks_during_url_competition,

  q.previous_impressions,
  q.current_impressions,

  q.current_impressions - q.previous_impressions AS impressions_change,

  q.previous_avg_position,
  q.current_avg_position,

  q.current_avg_position - q.previous_avg_position AS position_change,

  c.current_url_total_clicks,
  c.current_url_total_impressions,

  c.current_dominant_url_clicks,
  c.current_dominant_url_impressions,
  c.current_dominant_url_avg_position

FROM current_url_summary c
LEFT JOIN previous_dominant_url p
  USING (snapshot_date, query)

LEFT JOIN query_pivoted q
  USING (snapshot_date, query)

WHERE c.url_count >= 2
  AND q.current_impressions >= min_cannibalization_query_impressions;


-- ============================================================
-- PART 3. CTR ISSUES
-- ============================================================

-- 3.1 CTR position benchmarks from previous period

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_ctr_position_benchmarks`
PARTITION BY snapshot_date
CLUSTER BY position_bucket
AS

WITH latest_date AS (
  SELECT
    MAX(data_date) AS snapshot_date
  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
),

previous_period AS (
  SELECT
    l.snapshot_date,
    url,
    query,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SAFE_DIVIDE(SUM(sum_position), SUM(impressions)) + 1 AS avg_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
    AND query IS NOT NULL
    AND url IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    url,
    query
),

bucketed AS (
  SELECT
    snapshot_date,

    CASE
      WHEN avg_position >= 1 AND avg_position < 4 THEN '1-3'
      WHEN avg_position >= 4 AND avg_position < 7 THEN '4-6'
      WHEN avg_position >= 7 AND avg_position < 11 THEN '7-10'
      WHEN avg_position >= 11 AND avg_position < 21 THEN '11-20'
      ELSE '21+'
    END AS position_bucket,

    clicks,
    impressions

  FROM previous_period
  WHERE impressions >= min_ctr_current_impressions
)

SELECT
  snapshot_date,
  position_bucket,

  SUM(clicks) AS benchmark_clicks,
  SUM(impressions) AS benchmark_impressions,

  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS expected_ctr

FROM bucketed
GROUP BY
  snapshot_date,
  position_bucket

HAVING benchmark_impressions >= min_ctr_benchmark_impressions;


-- 3.2 Main CTR issues table

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_ctr_issues`
PARTITION BY snapshot_date
CLUSTER BY position_bucket
AS

WITH latest_date AS (
  SELECT
    MAX(data_date) AS snapshot_date
  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
),

current_period AS (
  SELECT
    l.snapshot_date,
    url,
    query,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS actual_ctr,
    SAFE_DIVIDE(SUM(sum_position), SUM(impressions)) + 1 AS avg_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                      AND l.snapshot_date
    AND query IS NOT NULL
    AND url IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    url,
    query
),

bucketed_current AS (
  SELECT
    *,

    CASE
      WHEN avg_position >= 1 AND avg_position < 4 THEN '1-3'
      WHEN avg_position >= 4 AND avg_position < 7 THEN '4-6'
      WHEN avg_position >= 7 AND avg_position < 11 THEN '7-10'
      WHEN avg_position >= 11 AND avg_position < 21 THEN '11-20'
      ELSE '21+'
    END AS position_bucket

  FROM current_period
  WHERE impressions >= min_ctr_current_impressions
),

with_benchmark AS (
  SELECT
    c.snapshot_date,
    c.url,
    c.query,
    c.position_bucket,

    c.clicks,
    c.impressions,
    c.actual_ctr,
    c.avg_position,

    b.expected_ctr,

    b.expected_ctr - c.actual_ctr AS ctr_gap,

    CAST(
      GREATEST(
        0,
        ROUND((b.expected_ctr - c.actual_ctr) * c.impressions)
      ) AS INT64
    ) AS estimated_lost_clicks

  FROM bucketed_current c
  LEFT JOIN `YOUR_PROJECT.leakonic.seo_ctr_position_benchmarks` b
    ON c.snapshot_date = b.snapshot_date
   AND c.position_bucket = b.position_bucket
)

SELECT
  *
FROM with_benchmark
WHERE expected_ctr IS NOT NULL
  AND ctr_gap >= min_ctr_gap
  AND estimated_lost_clicks > 0;


-- 3.3 CTR bucket comparison table

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_ctr_bucket_comparison`
PARTITION BY snapshot_date
CLUSTER BY position_bucket
AS

SELECT
  snapshot_date,
  position_bucket,

  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,

  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS actual_ctr,

  SAFE_DIVIDE(SUM(expected_ctr * impressions), SUM(impressions)) AS expected_ctr,

  SAFE_DIVIDE(SUM(expected_ctr * impressions), SUM(impressions))
    - SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr_gap,

  SUM(estimated_lost_clicks) AS estimated_lost_clicks

FROM `YOUR_PROJECT.leakonic.seo_ctr_issues`
GROUP BY
  snapshot_date,
  position_bucket;


-- 3.4 CTR scorecards

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_ctr_scorecards`
PARTITION BY snapshot_date
AS

SELECT
  snapshot_date,

  SUM(estimated_lost_clicks) AS estimated_lost_clicks_from_ctr_issues,

  COUNT(DISTINCT query) AS queries_with_ctr_issues,

  COUNT(DISTINCT url) AS urls_with_ctr_issues,

  COUNTIF(impressions >= 1000 AND actual_ctr < expected_ctr) AS high_impression_low_ctr_cases

FROM `YOUR_PROJECT.leakonic.seo_ctr_issues`
GROUP BY snapshot_date;


-- ============================================================
-- PART 4. CLICKS / TRAFFIC ISSUES
-- ============================================================

-- 4.1 Daily clicks trend vs previous period

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_traffic_daily_comparison`
PARTITION BY snapshot_date
AS

WITH latest_date AS (
  SELECT
    MAX(data_date) AS snapshot_date
  FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
),

daily AS (
  SELECT
    l.snapshot_date,
    data_date,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
    SAFE_DIVIDE(SUM(sum_top_position), SUM(impressions)) + 1 AS avg_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date

  GROUP BY
    l.snapshot_date,
    data_date
),

current_period AS (
  SELECT
    snapshot_date,
    data_date AS current_date,
    DATE_DIFF(data_date, DATE_SUB(snapshot_date, INTERVAL current_start_offset DAY), DAY) + 1 AS day_number,
    clicks AS current_clicks,
    impressions AS current_impressions,
    ctr AS current_ctr,
    avg_position AS current_avg_position

  FROM daily
  WHERE data_date BETWEEN DATE_SUB(snapshot_date, INTERVAL current_start_offset DAY)
                      AND snapshot_date
),

previous_period AS (
  SELECT
    snapshot_date,
    data_date AS previous_date,
    DATE_DIFF(data_date, DATE_SUB(snapshot_date, INTERVAL previous_start_offset DAY), DAY) + 1 AS day_number,
    clicks AS previous_clicks,
    impressions AS previous_impressions,
    ctr AS previous_ctr,
    avg_position AS previous_avg_position

  FROM daily
  WHERE data_date BETWEEN DATE_SUB(snapshot_date, INTERVAL previous_start_offset DAY)
                      AND DATE_SUB(snapshot_date, INTERVAL period_days DAY)
)

SELECT
  c.snapshot_date,
  c.day_number,

  c.current_date,
  p.previous_date,

  c.current_clicks,
  IFNULL(p.previous_clicks, 0) AS previous_clicks,

  c.current_clicks - IFNULL(p.previous_clicks, 0) AS click_change,

  c.current_impressions,
  IFNULL(p.previous_impressions, 0) AS previous_impressions,

  c.current_ctr,
  p.previous_ctr,

  c.current_avg_position,
  p.previous_avg_position

FROM current_period c
LEFT JOIN previous_period p
  USING (snapshot_date, day_number);


-- 4.2 URL traffic issues

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_traffic_url_issues`
PARTITION BY snapshot_date
CLUSTER BY lost_clicks
AS

WITH latest_date AS (
  SELECT
    MAX(data_date) AS snapshot_date
  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
),

periodized AS (
  SELECT
    l.snapshot_date,
    url,

    CASE
      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                         AND l.snapshot_date
        THEN 'current'

      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                         AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
        THEN 'previous'
    END AS period,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(sum_position) AS sum_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date
    AND url IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    url,
    period
),

pivoted AS (
  SELECT
    snapshot_date,
    url,

    SUM(IF(period = 'current', clicks, 0)) AS current_clicks,
    SUM(IF(period = 'previous', clicks, 0)) AS previous_clicks,

    SUM(IF(period = 'current', impressions, 0)) AS current_impressions,
    SUM(IF(period = 'previous', impressions, 0)) AS previous_impressions,

    SUM(IF(period = 'current', sum_position, 0)) AS current_sum_position,
    SUM(IF(period = 'previous', sum_position, 0)) AS previous_sum_position

  FROM periodized
  WHERE period IS NOT NULL
  GROUP BY
    snapshot_date,
    url
)

SELECT
  snapshot_date,
  url,

  previous_clicks,
  current_clicks,

  current_clicks - previous_clicks AS click_change,
  GREATEST(0, previous_clicks - current_clicks) AS lost_clicks,

  previous_impressions,
  current_impressions,
  current_impressions - previous_impressions AS impressions_change,

  SAFE_DIVIDE(previous_clicks, previous_impressions) AS previous_ctr,
  SAFE_DIVIDE(current_clicks, current_impressions) AS current_ctr,

  SAFE_DIVIDE(current_clicks, current_impressions)
    - SAFE_DIVIDE(previous_clicks, previous_impressions) AS ctr_change,

  SAFE_DIVIDE(previous_sum_position, previous_impressions) + 1 AS previous_avg_position,
  SAFE_DIVIDE(current_sum_position, current_impressions) + 1 AS current_avg_position,

  SAFE_DIVIDE(current_sum_position, current_impressions) + 1
    - (SAFE_DIVIDE(previous_sum_position, previous_impressions) + 1) AS position_change

FROM pivoted
WHERE GREATEST(0, previous_clicks - current_clicks) >= min_traffic_lost_clicks
  AND (
    current_impressions >= min_traffic_impressions
    OR previous_impressions >= min_traffic_impressions
  );


-- 4.3 Query traffic issues

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_traffic_query_issues`
PARTITION BY snapshot_date
CLUSTER BY lost_clicks
AS

WITH latest_date AS (
  SELECT
    MAX(data_date) AS snapshot_date
  FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
),

periodized AS (
  SELECT
    l.snapshot_date,
    query,

    CASE
      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                         AND l.snapshot_date
        THEN 'current'

      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                         AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
        THEN 'previous'
    END AS period,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(sum_top_position) AS sum_top_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date
    AND query IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    query,
    period
),

pivoted AS (
  SELECT
    snapshot_date,
    query,

    SUM(IF(period = 'current', clicks, 0)) AS current_clicks,
    SUM(IF(period = 'previous', clicks, 0)) AS previous_clicks,

    SUM(IF(period = 'current', impressions, 0)) AS current_impressions,
    SUM(IF(period = 'previous', impressions, 0)) AS previous_impressions,

    SUM(IF(period = 'current', sum_top_position, 0)) AS current_sum_top_position,
    SUM(IF(period = 'previous', sum_top_position, 0)) AS previous_sum_top_position

  FROM periodized
  WHERE period IS NOT NULL
  GROUP BY
    snapshot_date,
    query
)

SELECT
  snapshot_date,
  query,

  previous_clicks,
  current_clicks,

  current_clicks - previous_clicks AS click_change,
  GREATEST(0, previous_clicks - current_clicks) AS lost_clicks,

  previous_impressions,
  current_impressions,
  current_impressions - previous_impressions AS impressions_change,

  SAFE_DIVIDE(previous_clicks, previous_impressions) AS previous_ctr,
  SAFE_DIVIDE(current_clicks, current_impressions) AS current_ctr,

  SAFE_DIVIDE(current_clicks, current_impressions)
    - SAFE_DIVIDE(previous_clicks, previous_impressions) AS ctr_change,

  SAFE_DIVIDE(previous_sum_top_position, previous_impressions) + 1 AS previous_avg_position,
  SAFE_DIVIDE(current_sum_top_position, current_impressions) + 1 AS current_avg_position,

  SAFE_DIVIDE(current_sum_top_position, current_impressions) + 1
    - (SAFE_DIVIDE(previous_sum_top_position, previous_impressions) + 1) AS position_change

FROM pivoted
WHERE GREATEST(0, previous_clicks - current_clicks) >= min_traffic_lost_clicks
  AND (
    current_impressions >= min_traffic_impressions
    OR previous_impressions >= min_traffic_impressions
  );


-- 4.4 Main traffic leak table: URL + query

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_traffic_page_query_issues`
PARTITION BY snapshot_date
CLUSTER BY lost_clicks
AS

WITH latest_date AS (
  SELECT
    MAX(data_date) AS snapshot_date
  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
),

periodized AS (
  SELECT
    l.snapshot_date,
    url,
    query,

    CASE
      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                         AND l.snapshot_date
        THEN 'current'

      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                         AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
        THEN 'previous'
    END AS period,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(sum_position) AS sum_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date
    AND url IS NOT NULL
    AND query IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    url,
    query,
    period
),

pivoted AS (
  SELECT
    snapshot_date,
    url,
    query,

    SUM(IF(period = 'current', clicks, 0)) AS current_clicks,
    SUM(IF(period = 'previous', clicks, 0)) AS previous_clicks,

    SUM(IF(period = 'current', impressions, 0)) AS current_impressions,
    SUM(IF(period = 'previous', impressions, 0)) AS previous_impressions,

    SUM(IF(period = 'current', sum_position, 0)) AS current_sum_position,
    SUM(IF(period = 'previous', sum_position, 0)) AS previous_sum_position

  FROM periodized
  WHERE period IS NOT NULL
  GROUP BY
    snapshot_date,
    url,
    query
)

SELECT
  snapshot_date,
  url,
  query,

  previous_clicks,
  current_clicks,

  current_clicks - previous_clicks AS click_change,
  GREATEST(0, previous_clicks - current_clicks) AS lost_clicks,

  previous_impressions,
  current_impressions,
  current_impressions - previous_impressions AS impressions_change,

  SAFE_DIVIDE(previous_clicks, previous_impressions) AS previous_ctr,
  SAFE_DIVIDE(current_clicks, current_impressions) AS current_ctr,

  SAFE_DIVIDE(current_clicks, current_impressions)
    - SAFE_DIVIDE(previous_clicks, previous_impressions) AS ctr_change,

  SAFE_DIVIDE(previous_sum_position, previous_impressions) + 1 AS previous_avg_position,
  SAFE_DIVIDE(current_sum_position, current_impressions) + 1 AS current_avg_position,

  SAFE_DIVIDE(current_sum_position, current_impressions) + 1
    - (SAFE_DIVIDE(previous_sum_position, previous_impressions) + 1) AS position_change

FROM pivoted
WHERE GREATEST(0, previous_clicks - current_clicks) >= min_traffic_lost_clicks
  AND (
    current_impressions >= min_traffic_impressions
    OR previous_impressions >= min_traffic_impressions
  );


-- 4.5 Traffic issue scorecards

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_traffic_scorecards`
PARTITION BY snapshot_date
AS

WITH site_totals AS (
  SELECT
    snapshot_date,

    SUM(current_clicks) AS current_clicks,
    SUM(previous_clicks) AS previous_clicks

  FROM `YOUR_PROJECT.leakonic.seo_traffic_daily_comparison`
  GROUP BY snapshot_date
),

url_counts AS (
  SELECT
    snapshot_date,
    COUNT(DISTINCT url) AS urls_losing_clicks
  FROM `YOUR_PROJECT.leakonic.seo_traffic_url_issues`
  GROUP BY snapshot_date
),

query_counts AS (
  SELECT
    snapshot_date,
    COUNT(DISTINCT query) AS queries_losing_clicks
  FROM `YOUR_PROJECT.leakonic.seo_traffic_query_issues`
  GROUP BY snapshot_date
)

SELECT
  s.snapshot_date,

  s.previous_clicks,
  s.current_clicks,

  s.current_clicks - s.previous_clicks AS click_change,

  GREATEST(0, s.previous_clicks - s.current_clicks) AS lost_clicks,

  IFNULL(u.urls_losing_clicks, 0) AS urls_losing_clicks,

  IFNULL(q.queries_losing_clicks, 0) AS queries_losing_clicks

FROM site_totals s
LEFT JOIN url_counts u
  USING (snapshot_date)
LEFT JOIN query_counts q
  USING (snapshot_date);


-- ============================================================
-- PART 5. KEYWORD POSITION ISSUES
-- ============================================================

-- 5.1 Keyword comparison table

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_position_keyword_comparison`
PARTITION BY snapshot_date
CLUSTER BY current_position_bucket
AS

WITH latest_date AS (
  SELECT
    MAX(data_date) AS snapshot_date
  FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
),

periodized AS (
  SELECT
    l.snapshot_date,
    query,

    CASE
      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                         AND l.snapshot_date
        THEN 'current'

      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                         AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
        THEN 'previous'
    END AS period,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(sum_top_position) AS sum_top_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_site_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date
    AND query IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    query,
    period
),

pivoted AS (
  SELECT
    snapshot_date,
    query,

    SUM(IF(period = 'current', clicks, 0)) AS current_clicks,
    SUM(IF(period = 'previous', clicks, 0)) AS previous_clicks,

    SUM(IF(period = 'current', impressions, 0)) AS current_impressions,
    SUM(IF(period = 'previous', impressions, 0)) AS previous_impressions,

    SUM(IF(period = 'current', sum_top_position, 0)) AS current_sum_top_position,
    SUM(IF(period = 'previous', sum_top_position, 0)) AS previous_sum_top_position

  FROM periodized
  WHERE period IS NOT NULL
  GROUP BY
    snapshot_date,
    query
),

calculated AS (
  SELECT
    snapshot_date,
    query,

    previous_clicks,
    current_clicks,
    current_clicks - previous_clicks AS click_change,
    GREATEST(0, previous_clicks - current_clicks) AS lost_clicks,

    previous_impressions,
    current_impressions,
    current_impressions - previous_impressions AS impressions_change,

    SAFE_DIVIDE(previous_clicks, previous_impressions) AS previous_ctr,
    SAFE_DIVIDE(current_clicks, current_impressions) AS current_ctr,

    SAFE_DIVIDE(current_clicks, current_impressions)
      - SAFE_DIVIDE(previous_clicks, previous_impressions) AS ctr_change,

    SAFE_DIVIDE(previous_sum_top_position, previous_impressions) + 1 AS previous_avg_position,
    SAFE_DIVIDE(current_sum_top_position, current_impressions) + 1 AS current_avg_position,

    SAFE_DIVIDE(current_sum_top_position, current_impressions) + 1
      - (SAFE_DIVIDE(previous_sum_top_position, previous_impressions) + 1) AS position_change

  FROM pivoted
)

SELECT
  *,

  IFNULL(previous_avg_position, 101) AS previous_position_for_bucket,
  IFNULL(current_avg_position, 101) AS current_position_for_bucket,

  CASE
    WHEN previous_avg_position IS NULL THEN 'No previous data'
    WHEN previous_avg_position >= 1 AND previous_avg_position < 4 THEN '1-3'
    WHEN previous_avg_position >= 4 AND previous_avg_position < 11 THEN '4-10'
    WHEN previous_avg_position >= 11 AND previous_avg_position < 21 THEN '11-20'
    WHEN previous_avg_position >= 21 AND previous_avg_position < 51 THEN '21-50'
    ELSE '51+'
  END AS previous_position_bucket,

  CASE
    WHEN current_avg_position IS NULL THEN 'No current data'
    WHEN current_avg_position >= 1 AND current_avg_position < 4 THEN '1-3'
    WHEN current_avg_position >= 4 AND current_avg_position < 11 THEN '4-10'
    WHEN current_avg_position >= 11 AND current_avg_position < 21 THEN '11-20'
    WHEN current_avg_position >= 21 AND current_avg_position < 51 THEN '21-50'
    ELSE '51+'
  END AS current_position_bucket

FROM calculated
WHERE current_impressions >= min_position_impressions
   OR previous_impressions >= min_position_impressions;


-- 5.2 Keyword position distribution

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_position_bucket_distribution`
PARTITION BY snapshot_date
CLUSTER BY period, position_bucket
AS

SELECT
  snapshot_date,
  'previous' AS period,
  previous_position_bucket AS position_bucket,

  CASE
    WHEN previous_position_bucket = '1-3' THEN 1
    WHEN previous_position_bucket = '4-10' THEN 2
    WHEN previous_position_bucket = '11-20' THEN 3
    WHEN previous_position_bucket = '21-50' THEN 4
    WHEN previous_position_bucket = '51+' THEN 5
    ELSE 6
  END AS position_bucket_order,

  COUNT(DISTINCT query) AS keyword_count,
  SUM(previous_clicks) AS clicks,
  SUM(previous_impressions) AS impressions

FROM `YOUR_PROJECT.leakonic.seo_position_keyword_comparison`
GROUP BY
  snapshot_date,
  period,
  position_bucket,
  position_bucket_order

UNION ALL

SELECT
  snapshot_date,
  'current' AS period,
  current_position_bucket AS position_bucket,

  CASE
    WHEN current_position_bucket = '1-3' THEN 1
    WHEN current_position_bucket = '4-10' THEN 2
    WHEN current_position_bucket = '11-20' THEN 3
    WHEN current_position_bucket = '21-50' THEN 4
    WHEN current_position_bucket = '51+' THEN 5
    ELSE 6
  END AS position_bucket_order,

  COUNT(DISTINCT query) AS keyword_count,
  SUM(current_clicks) AS clicks,
  SUM(current_impressions) AS impressions

FROM `YOUR_PROJECT.leakonic.seo_position_keyword_comparison`
GROUP BY
  snapshot_date,
  period,
  position_bucket,
  position_bucket_order;


-- 5.3 Keywords dropped from important positions

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_position_drop_types`
PARTITION BY snapshot_date
CLUSTER BY drop_type
AS

SELECT
  snapshot_date,
  'Dropped from Top 3' AS drop_type,
  1 AS drop_type_order,
  COUNT(DISTINCT query) AS keyword_count,
  SUM(lost_clicks) AS lost_clicks

FROM `YOUR_PROJECT.leakonic.seo_position_keyword_comparison`
WHERE previous_position_for_bucket < 4
  AND current_position_for_bucket >= 4
GROUP BY snapshot_date

UNION ALL

SELECT
  snapshot_date,
  'Dropped from Top 10' AS drop_type,
  2 AS drop_type_order,
  COUNT(DISTINCT query) AS keyword_count,
  SUM(lost_clicks) AS lost_clicks

FROM `YOUR_PROJECT.leakonic.seo_position_keyword_comparison`
WHERE previous_position_for_bucket < 11
  AND current_position_for_bucket >= 11
GROUP BY snapshot_date

UNION ALL

SELECT
  snapshot_date,
  'Dropped from Top 20' AS drop_type,
  3 AS drop_type_order,
  COUNT(DISTINCT query) AS keyword_count,
  SUM(lost_clicks) AS lost_clicks

FROM `YOUR_PROJECT.leakonic.seo_position_keyword_comparison`
WHERE previous_position_for_bucket < 21
  AND current_position_for_bucket >= 21
GROUP BY snapshot_date

UNION ALL

SELECT
  snapshot_date,
  'Dropped beyond 50 / no current data' AS drop_type,
  4 AS drop_type_order,
  COUNT(DISTINCT query) AS keyword_count,
  SUM(lost_clicks) AS lost_clicks

FROM `YOUR_PROJECT.leakonic.seo_position_keyword_comparison`
WHERE previous_position_for_bucket < 51
  AND current_position_for_bucket >= 51
GROUP BY snapshot_date;


-- 5.4 Main ranking drop table: query + URL

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_position_issues`
PARTITION BY snapshot_date
CLUSTER BY drop_type
AS

WITH latest_date AS (
  SELECT
    MAX(data_date) AS snapshot_date
  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
),

periodized AS (
  SELECT
    l.snapshot_date,
    query,
    url,

    CASE
      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL current_start_offset DAY)
                         AND l.snapshot_date
        THEN 'current'

      WHEN data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                         AND DATE_SUB(l.snapshot_date, INTERVAL period_days DAY)
        THEN 'previous'
    END AS period,

    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(sum_position) AS sum_position

  FROM `YOUR_PROJECT.searchconsole.searchdata_url_impression`
  CROSS JOIN latest_date l

  WHERE data_date BETWEEN DATE_SUB(l.snapshot_date, INTERVAL previous_start_offset DAY)
                      AND l.snapshot_date
    AND query IS NOT NULL
    AND url IS NOT NULL
    AND impressions > 0

  GROUP BY
    l.snapshot_date,
    query,
    url,
    period
),

pivoted AS (
  SELECT
    snapshot_date,
    query,
    url,

    SUM(IF(period = 'current', clicks, 0)) AS current_clicks,
    SUM(IF(period = 'previous', clicks, 0)) AS previous_clicks,

    SUM(IF(period = 'current', impressions, 0)) AS current_impressions,
    SUM(IF(period = 'previous', impressions, 0)) AS previous_impressions,

    SUM(IF(period = 'current', sum_position, 0)) AS current_sum_position,
    SUM(IF(period = 'previous', sum_position, 0)) AS previous_sum_position

  FROM periodized
  WHERE period IS NOT NULL
  GROUP BY
    snapshot_date,
    query,
    url
),

calculated AS (
  SELECT
    snapshot_date,
    query,
    url,

    previous_clicks,
    current_clicks,
    current_clicks - previous_clicks AS click_change,
    GREATEST(0, previous_clicks - current_clicks) AS lost_clicks,

    previous_impressions,
    current_impressions,
    current_impressions - previous_impressions AS impressions_change,

    SAFE_DIVIDE(previous_clicks, previous_impressions) AS previous_ctr,
    SAFE_DIVIDE(current_clicks, current_impressions) AS current_ctr,

    SAFE_DIVIDE(current_clicks, current_impressions)
      - SAFE_DIVIDE(previous_clicks, previous_impressions) AS ctr_change,

    SAFE_DIVIDE(previous_sum_position, previous_impressions) + 1 AS previous_avg_position,
    SAFE_DIVIDE(current_sum_position, current_impressions) + 1 AS current_avg_position,

    SAFE_DIVIDE(current_sum_position, current_impressions) + 1
      - (SAFE_DIVIDE(previous_sum_position, previous_impressions) + 1) AS position_change

  FROM pivoted
),

with_effective_positions AS (
  SELECT
    *,

    IFNULL(previous_avg_position, 101) AS previous_position_for_drop,
    IFNULL(current_avg_position, 101) AS current_position_for_drop

  FROM calculated
)

SELECT
  *,

  CASE
    WHEN previous_position_for_drop < 4
      AND current_position_for_drop >= 4
      THEN 'Dropped from Top 3'

    WHEN previous_position_for_drop < 11
      AND current_position_for_drop >= 11
      THEN 'Dropped from Top 10'

    WHEN previous_position_for_drop < 21
      AND current_position_for_drop >= 21
      THEN 'Dropped from Top 20'

    WHEN previous_position_for_drop < 51
      AND current_position_for_drop >= 51
      THEN 'Dropped beyond 50 / no current data'

    ELSE 'Ranking drop'
  END AS drop_type

FROM with_effective_positions
WHERE current_position_for_drop - previous_position_for_drop >= min_position_drop
  AND lost_clicks >= min_position_lost_clicks
  AND (
    current_impressions >= min_position_impressions
    OR previous_impressions >= min_position_impressions
  );


-- 5.5 Position issue scorecards

CREATE OR REPLACE TABLE `YOUR_PROJECT.leakonic.seo_position_scorecards`
PARTITION BY snapshot_date
AS

WITH issue_summary AS (
  SELECT
    snapshot_date,

    COUNT(DISTINCT query) AS queries_with_position_drops,
    COUNT(DISTINCT url) AS urls_affected_by_ranking_drops,
    SUM(lost_clicks) AS lost_clicks_from_ranking_drops

  FROM `YOUR_PROJECT.leakonic.seo_position_issues`
  GROUP BY snapshot_date
),

top10_drops AS (
  SELECT
    snapshot_date,
    keyword_count AS keywords_dropped_out_of_top_10

  FROM `YOUR_PROJECT.leakonic.seo_position_drop_types`
  WHERE drop_type = 'Dropped from Top 10'
)

SELECT
  i.snapshot_date,

  i.queries_with_position_drops,
  i.urls_affected_by_ranking_drops,
  i.lost_clicks_from_ranking_drops,

  IFNULL(t.keywords_dropped_out_of_top_10, 0) AS keywords_dropped_out_of_top_10

FROM issue_summary i
LEFT JOIN top10_drops t
  USING (snapshot_date);
