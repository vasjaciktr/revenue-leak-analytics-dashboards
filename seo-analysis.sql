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
    COUNT(DISTINCT query) AS queries_with_issues
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
