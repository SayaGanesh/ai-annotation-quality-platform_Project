-- ============================================================
-- File: reporting_views.sql
-- Project: AI Training Data Quality & Annotation Analytics Platform
-- Purpose: Power BI reporting views
-- ============================================================

-- ── VIEW 1: Annotation Quality Overview ───────────────────────
CREATE OR ALTER VIEW reporting.vw_quality_overview AS
SELECT
    p.project_name,
    p.client_name,
    d.full_date,
    d.month_name,
    d.year,
    f.total_annotations,
    f.high_quality_count,
    f.acceptable_count,
    f.flagged_count,
    f.rejected_count,
    ROUND(f.avg_quality_score, 2) AS avg_quality_score,
    ROUND(f.rejection_rate * 100, 2) AS rejection_rate_pct,
    -- 7-day moving avg quality
    ROUND(
        AVG(f.avg_quality_score) OVER (
            PARTITION BY f.project_key
            ORDER BY d.full_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    ) AS quality_7d_avg,
    -- WoW quality change
    ROUND(
        f.avg_quality_score -
        LAG(f.avg_quality_score) OVER (
            PARTITION BY f.project_key ORDER BY d.full_date
        ), 2
    ) AS wow_quality_change
FROM fact.daily_annotator_quality f
JOIN dim.project p ON f.project_key = p.project_key
JOIN dim.date    d ON f.date_key    = d.date_key;
GO

-- ── VIEW 2: Annotator Leaderboard ─────────────────────────────
CREATE OR ALTER VIEW reporting.vw_annotator_leaderboard AS
SELECT
    a.annotator_name,
    a.team_name,
    a.experience_level,
    p.project_name,
    d.full_date,
    f.total_annotations,
    ROUND(f.avg_quality_score, 2)    AS avg_quality_score,
    ROUND(f.rejection_rate * 100, 2) AS rejection_rate_pct,
    f.wow_quality_change_pct,
    -- Rank within team per day
    DENSE_RANK() OVER (
        PARTITION BY a.team_id, f.project_key, f.date_key
        ORDER BY f.avg_quality_score DESC
    ) AS team_rank
FROM fact.daily_annotator_quality f
JOIN dim.annotator a ON f.annotator_key = a.annotator_key
JOIN dim.project   p ON f.project_key   = p.project_key
JOIN dim.date      d ON f.date_key      = d.date_key;
GO

-- ── VIEW 3: Dataset Readiness Tracker ─────────────────────────
CREATE OR ALTER VIEW reporting.vw_dataset_readiness AS
SELECT
    p.project_name,
    p.dataset_purpose,
    d.full_date,
    f.total_frames,
    f.annotated_frames,
    f.qa_passed_frames,
    ROUND(f.annotation_coverage_pct, 2) AS annotation_coverage_pct,
    ROUND(f.qa_pass_rate_pct, 2)        AS qa_pass_rate_pct,
    f.ready_for_training,
    CASE
        WHEN f.ready_for_training = 1 THEN '✅ Ready'
        WHEN f.qa_pass_rate_pct >= 75  THEN '🟡 Near Ready'
        ELSE '🔴 Not Ready'
    END AS readiness_status
FROM fact.dataset_readiness f
JOIN dim.project p ON f.project_key = p.project_key
JOIN dim.date    d ON f.date_key    = d.date_key;
GO

-- ── VIEW 4: Quality Distribution Breakdown ────────────────────
CREATE OR ALTER VIEW reporting.vw_quality_distribution AS
SELECT
    p.project_name,
    d.full_date,
    quality_tier,
    COUNT(annotation_key) AS annotation_count,
    ROUND(
        COUNT(annotation_key) * 100.0 /
        SUM(COUNT(annotation_key)) OVER (
            PARTITION BY f.project_key, f.date_key
        ), 2
    ) AS pct_of_total
FROM fact.annotation_events f
JOIN dim.project p ON f.project_key = p.project_key
JOIN dim.date    d ON f.date_key    = d.date_key
GROUP BY p.project_name, d.full_date, f.quality_tier, f.project_key, f.date_key;
GO
