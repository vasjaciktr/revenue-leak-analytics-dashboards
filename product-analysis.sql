-- 8. Product Analysis - Medium Package
-- Creates issue-focused product analysis tables:
-- 8A. product_session_funnel
-- 8B. product_performance
-- 8C. product_view_to_cart_issues
-- 8D. product_cart_to_purchase_issues
-- 8E. category_view_to_purchase_issues
-- 8F. category_cart_to_purchase_issues
-- 8G. top_sellers_with_lost_revenue

-- 0. Create dataset (schema)

CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.leakonic`;

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
