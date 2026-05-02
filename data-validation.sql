-- 7. Validation checks

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
