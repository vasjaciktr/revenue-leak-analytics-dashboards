-- ============================================================
-- LEAKONIC PREMIUM PACKAGE
-- GOOGLE ADS BUDGET LEAKS SECTION
-- ============================================================
--
-- Output tables created:
-- 1. europafoodxb-450709.leakonic.google_ads_campaign_performance
-- 2. europafoodxb-450709.leakonic.google_ads_campaign_leak_signals
-- 3. europafoodxb-450709.leakonic.google_ads_overview_scorecards
--
-- Replace:
-- europafoodxb-450709
-- google_ads
--
-- This version works with Google Ads transfer views like:
-- ads_CampaignBasicStats_5457744553
-- ads_Campaign_5457744553
--
-- It avoids wildcard querying because Google Ads transfer creates views,
-- and BigQuery cannot query views through a wildcard prefix.

-- ============================================================
-- 0. FIND GOOGLE ADS SOURCE VIEWS AND MATERIALIZE TEMP TABLES
-- ============================================================

DECLARE campaign_basic_stats_sql STRING;
DECLARE campaign_table_sql STRING;

SET campaign_basic_stats_sql = (
  SELECT
    STRING_AGG(
      FORMAT(
        'SELECT * FROM `europafoodxb-450709.google_ads.%s`',
        table_name
      ),
      ' UNION ALL '
    )
  FROM `europafoodxb-450709.google_ads.INFORMATION_SCHEMA.TABLES`
  WHERE REGEXP_CONTAINS(table_name, '^ads_CampaignBasicStats_[0-9]+$')
);

SET campaign_table_sql = (
  SELECT
    STRING_AGG(
      FORMAT(
        'SELECT * FROM `europafoodxb-450709.google_ads.%s`',
        table_name
      ),
      ' UNION ALL '
    )
  FROM `europafoodxb-450709.google_ads.INFORMATION_SCHEMA.TABLES`
  WHERE REGEXP_CONTAINS(table_name, '^ads_Campaign_[0-9]+$')
);

ASSERT campaign_basic_stats_sql IS NOT NULL
  AS 'No ads_CampaignBasicStats views found in google_ads';

ASSERT campaign_table_sql IS NOT NULL
  AS 'No ads_Campaign views found in google_ads';

EXECUTE IMMEDIATE
  'CREATE TEMP TABLE google_ads_campaign_basic_stats AS ' || campaign_basic_stats_sql;

EXECUTE IMMEDIATE
  'CREATE TEMP TABLE google_ads_campaigns AS ' || campaign_table_sql;


-- ============================================================
-- 1. GOOGLE ADS CAMPAIGN PERFORMANCE
-- ============================================================

CREATE OR REPLACE TABLE `europafoodxb-450709.leakonic.google_ads_campaign_performance` AS

WITH date_params AS (
  SELECT
    MAX(CAST(segments_date AS DATE)) AS end_date,
    DATE_SUB(MAX(CAST(segments_date AS DATE)), INTERVAL 13 DAY) AS start_date
  FROM google_ads_campaign_basic_stats
),

campaign_names AS (
  SELECT
    customer_id,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_advertising_channel_type AS campaign_type
  FROM google_ads_campaigns
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id, campaign_id
    ORDER BY _DATA_DATE DESC
  ) = 1
),

daily_campaign_stats AS (
  SELECT
    CAST(s.segments_date AS DATE) AS date,

    CASE
      WHEN CAST(s.segments_date AS DATE)
        BETWEEN DATE_SUB(p.end_date, INTERVAL 6 DAY) AND p.end_date
        THEN 'Current 7 Days'
      ELSE 'Previous 7 Days'
    END AS period,

    CASE
      WHEN CAST(s.segments_date AS DATE)
        BETWEEN DATE_SUB(p.end_date, INTERVAL 6 DAY) AND p.end_date
        THEN 2
      ELSE 1
    END AS period_sort,

    s.customer_id,
    s.campaign_id,

    SUM(s.metrics_impressions) AS impressions,
    SUM(s.metrics_clicks) AS clicks,
    CAST(SUM(s.metrics_cost_micros) AS FLOAT64) / 1000000.0 AS cost,
    CAST(SUM(s.metrics_conversions) AS FLOAT64) AS conversions,
    CAST(SUM(s.metrics_conversions_value) AS FLOAT64) AS conversion_value

  FROM google_ads_campaign_basic_stats s
  CROSS JOIN date_params p
  WHERE CAST(s.segments_date AS DATE) BETWEEN p.start_date AND p.end_date
  GROUP BY
    date,
    period,
    period_sort,
    s.customer_id,
    s.campaign_id
)

SELECT
  d.date,
  d.period,
  d.period_sort,

  d.customer_id,
  d.campaign_id,

  COALESCE(
    n.campaign_name,
    CONCAT('Campaign ', CAST(d.campaign_id AS STRING))
  ) AS campaign_name,

  n.campaign_status,
  n.campaign_type,

  d.impressions,
  d.clicks,
  d.cost,
  d.conversions,
  d.conversion_value,

  SAFE_DIVIDE(d.clicks, d.impressions) AS ctr,
  SAFE_DIVIDE(d.cost, d.clicks) AS avg_cpc,
  SAFE_DIVIDE(d.conversions, d.clicks) AS conversion_rate,
  SAFE_DIVIDE(d.cost, d.conversions) AS cost_per_conversion,
  SAFE_DIVIDE(d.conversion_value, d.cost) AS roas

FROM daily_campaign_stats d
LEFT JOIN campaign_names n
  ON d.customer_id = n.customer_id
  AND d.campaign_id = n.campaign_id;


-- ============================================================
-- 2. GOOGLE ADS CAMPAIGN LEAK SIGNALS
-- ============================================================

CREATE OR REPLACE TABLE `europafoodxb-450709.leakonic.google_ads_campaign_leak_signals` AS

WITH settings AS (
  SELECT
    10.0 AS min_cost_for_signal,
    1.0 AS target_roas,
    0.10 AS spend_increase_threshold,
    0.25 AS cpa_increase_threshold,
    500 AS high_impressions_threshold,
    0.01 AS low_ctr_threshold
),

period_summary AS (
  SELECT
    customer_id,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_type,
    period,

    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS cost,
    SUM(conversions) AS conversions,
    SUM(conversion_value) AS conversion_value,

    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
    SAFE_DIVIDE(SUM(cost), SUM(clicks)) AS avg_cpc,
    SAFE_DIVIDE(SUM(conversions), SUM(clicks)) AS conversion_rate,
    SAFE_DIVIDE(SUM(cost), SUM(conversions)) AS cost_per_conversion,
    SAFE_DIVIDE(SUM(conversion_value), SUM(cost)) AS roas

  FROM `europafoodxb-450709.leakonic.google_ads_campaign_performance`
  GROUP BY
    customer_id,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_type,
    period
),

current_period AS (
  SELECT *
  FROM period_summary
  WHERE period = 'Current 7 Days'
),

previous_period AS (
  SELECT *
  FROM period_summary
  WHERE period = 'Previous 7 Days'
),

joined AS (
  SELECT
    c.customer_id,
    c.campaign_id,
    c.campaign_name,
    c.campaign_status,
    c.campaign_type,

    c.impressions AS current_impressions,
    COALESCE(p.impressions, 0) AS previous_impressions,

    c.clicks AS current_clicks,
    COALESCE(p.clicks, 0) AS previous_clicks,

    c.cost AS current_cost,
    COALESCE(p.cost, 0) AS previous_cost,

    c.conversions AS current_conversions,
    COALESCE(p.conversions, 0) AS previous_conversions,

    c.conversion_value AS current_conversion_value,
    COALESCE(p.conversion_value, 0) AS previous_conversion_value,

    c.ctr AS current_ctr,
    p.ctr AS previous_ctr,

    c.avg_cpc AS current_avg_cpc,
    p.avg_cpc AS previous_avg_cpc,

    c.conversion_rate AS current_conversion_rate,
    p.conversion_rate AS previous_conversion_rate,

    c.cost_per_conversion AS current_cost_per_conversion,
    p.cost_per_conversion AS previous_cost_per_conversion,

    c.roas AS current_roas,
    p.roas AS previous_roas

  FROM current_period c
  LEFT JOIN previous_period p
    ON c.customer_id = p.customer_id
    AND c.campaign_id = p.campaign_id
),

classified AS (
  SELECT
    j.*,

    CASE
      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_conversions = 0
        THEN 1

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.previous_cost > 0
        AND j.current_cost > j.previous_cost * (1 + s.spend_increase_threshold)
        AND j.current_conversions < j.previous_conversions
        THEN 2

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_conversions > 0
        AND j.previous_conversions > 0
        AND j.current_cost_per_conversion > j.previous_cost_per_conversion * (1 + s.cpa_increase_threshold)
        THEN 3

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_conversion_value > 0
        AND j.current_roas < s.target_roas
        THEN 4

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_impressions >= s.high_impressions_threshold
        AND j.current_ctr < s.low_ctr_threshold
        THEN 5

      ELSE 99
    END AS signal_priority,

    CASE
      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_conversions = 0
        THEN 'Spend with no conversions'

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.previous_cost > 0
        AND j.current_cost > j.previous_cost * (1 + s.spend_increase_threshold)
        AND j.current_conversions < j.previous_conversions
        THEN 'Spend increased but conversions dropped'

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_conversions > 0
        AND j.previous_conversions > 0
        AND j.current_cost_per_conversion > j.previous_cost_per_conversion * (1 + s.cpa_increase_threshold)
        THEN 'CPA increased'

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_conversion_value > 0
        AND j.current_roas < s.target_roas
        THEN 'Low ROAS'

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_impressions >= s.high_impressions_threshold
        AND j.current_ctr < s.low_ctr_threshold
        THEN 'Low CTR on high impressions'

      ELSE 'No major issue'
    END AS leak_type,

    CASE
      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_conversions = 0
        THEN j.current_cost

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.previous_cost > 0
        AND j.current_cost > j.previous_cost * (1 + s.spend_increase_threshold)
        AND j.current_conversions < j.previous_conversions
        THEN GREATEST(j.current_cost - j.previous_cost, 0)

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_conversions > 0
        AND j.previous_conversions > 0
        AND j.current_cost_per_conversion > j.previous_cost_per_conversion * (1 + s.cpa_increase_threshold)
        THEN GREATEST(j.current_cost - (j.current_conversions * j.previous_cost_per_conversion), 0)

      WHEN j.current_cost >= s.min_cost_for_signal
        AND j.current_conversion_value > 0
        AND j.current_roas < s.target_roas
        THEN GREATEST(j.current_cost - j.current_conversion_value, 0)

      ELSE 0
    END AS estimated_wasted_spend,

    CASE
      WHEN j.current_cost >= s.min_cost_for_signal
        THEN j.current_cost
      ELSE 0
    END AS affected_spend

  FROM joined j
  CROSS JOIN settings s
)

SELECT
  *
FROM classified
WHERE leak_type != 'No major issue'
ORDER BY
  estimated_wasted_spend DESC,
  affected_spend DESC,
  signal_priority ASC;


-- ============================================================
-- 3. GOOGLE ADS OVERVIEW SCORECARDS
-- ============================================================

CREATE OR REPLACE TABLE `europafoodxb-450709.leakonic.google_ads_overview_scorecards` AS

WITH period_summary AS (
  SELECT
    period,

    MIN(date) AS period_start_date,
    MAX(date) AS period_end_date,

    SUM(cost) AS ad_spend,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(conversions) AS conversions,
    SUM(conversion_value) AS conversion_value,

    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
    SAFE_DIVIDE(SUM(cost), SUM(clicks)) AS avg_cpc,
    SAFE_DIVIDE(SUM(conversions), SUM(clicks)) AS conversion_rate,
    SAFE_DIVIDE(SUM(cost), SUM(conversions)) AS cost_per_conversion,
    SAFE_DIVIDE(SUM(conversion_value), SUM(cost)) AS roas

  FROM `europafoodxb-450709.leakonic.google_ads_campaign_performance`
  GROUP BY period
),

current_period AS (
  SELECT *
  FROM period_summary
  WHERE period = 'Current 7 Days'
),

previous_period AS (
  SELECT *
  FROM period_summary
  WHERE period = 'Previous 7 Days'
),

waste_summary AS (
  SELECT
    SUM(estimated_wasted_spend) AS estimated_wasted_spend,
    SUM(affected_spend) AS affected_spend,
    COUNT(*) AS number_of_leak_signals,
    COUNT(DISTINCT campaign_id) AS leaking_campaigns
  FROM `europafoodxb-450709.leakonic.google_ads_campaign_leak_signals`
)

SELECT
  c.period_start_date AS current_start_date,
  c.period_end_date AS current_end_date,

  p.period_start_date AS previous_start_date,
  p.period_end_date AS previous_end_date,

  c.ad_spend AS current_ad_spend,
  p.ad_spend AS previous_ad_spend,
  SAFE_DIVIDE(c.ad_spend - p.ad_spend, p.ad_spend) AS ad_spend_change_pct,

  c.clicks AS current_clicks,
  p.clicks AS previous_clicks,
  SAFE_DIVIDE(c.clicks - p.clicks, p.clicks) AS clicks_change_pct,

  c.impressions AS current_impressions,
  p.impressions AS previous_impressions,
  SAFE_DIVIDE(c.impressions - p.impressions, p.impressions) AS impressions_change_pct,

  c.conversions AS current_conversions,
  p.conversions AS previous_conversions,
  SAFE_DIVIDE(c.conversions - p.conversions, p.conversions) AS conversions_change_pct,

  c.conversion_value AS current_conversion_value,
  p.conversion_value AS previous_conversion_value,
  SAFE_DIVIDE(c.conversion_value - p.conversion_value, p.conversion_value) AS conversion_value_change_pct,

  c.ctr AS current_ctr,
  p.ctr AS previous_ctr,
  SAFE_DIVIDE(c.ctr - p.ctr, p.ctr) AS ctr_change_pct,

  c.avg_cpc AS current_avg_cpc,
  p.avg_cpc AS previous_avg_cpc,
  SAFE_DIVIDE(c.avg_cpc - p.avg_cpc, p.avg_cpc) AS avg_cpc_change_pct,

  c.conversion_rate AS current_conversion_rate,
  p.conversion_rate AS previous_conversion_rate,
  SAFE_DIVIDE(c.conversion_rate - p.conversion_rate, p.conversion_rate) AS conversion_rate_change_pct,

  c.cost_per_conversion AS current_cost_per_conversion,
  p.cost_per_conversion AS previous_cost_per_conversion,
  SAFE_DIVIDE(c.cost_per_conversion - p.cost_per_conversion, p.cost_per_conversion) AS cost_per_conversion_change_pct,

  c.roas AS current_roas,
  p.roas AS previous_roas,
  SAFE_DIVIDE(c.roas - p.roas, p.roas) AS roas_change_pct,

  COALESCE(w.estimated_wasted_spend, 0) AS estimated_wasted_spend,
  COALESCE(w.affected_spend, 0) AS affected_spend,
  COALESCE(w.number_of_leak_signals, 0) AS number_of_leak_signals,
  COALESCE(w.leaking_campaigns, 0) AS leaking_campaigns,

  SAFE_DIVIDE(COALESCE(w.estimated_wasted_spend, 0), c.ad_spend) AS estimated_wasted_spend_share,

  CASE
    WHEN c.conversions > 0 AND c.conversion_value = 0
      THEN 'Conversion value may be missing'
    WHEN c.conversion_value > 0
      THEN 'Conversion value available'
    ELSE 'No conversion value detected'
  END AS conversion_value_status

FROM current_period c
LEFT JOIN previous_period p
  ON 1 = 1
LEFT JOIN waste_summary w
  ON 1 = 1;
