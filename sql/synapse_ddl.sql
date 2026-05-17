-- ============================================================
-- File: synapse_ddl.sql
-- Project: AI Training Data Quality & Annotation Analytics Platform
-- Purpose: Star schema for annotation quality analytics
-- ============================================================

CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS reporting;
GO

-- ── DIMENSION: Annotator ──────────────────────────────────────
CREATE TABLE dim.annotator (
    annotator_key       INT           NOT NULL,
    annotator_id        VARCHAR(100)  NOT NULL,
    annotator_name      VARCHAR(200),
    team_id             VARCHAR(50),
    team_name           VARCHAR(100),
    region              VARCHAR(50),
    annotation_type     VARCHAR(100),   -- bbox | polygon | keypoint
    experience_level    VARCHAR(20),    -- junior | mid | senior
    onboarded_date      DATE,
    is_active           BIT            DEFAULT 1
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);
GO

-- ── DIMENSION: Project ────────────────────────────────────────
CREATE TABLE dim.project (
    project_key         INT           NOT NULL,
    project_id          VARCHAR(100)  NOT NULL,
    project_name        VARCHAR(500),
    client_name         VARCHAR(200),
    model_type          VARCHAR(100),   -- detection | segmentation | classification
    dataset_purpose     VARCHAR(100),   -- training | validation | test
    start_date          DATE,
    target_end_date     DATE,
    is_active           BIT
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);
GO

-- ── DIMENSION: Date ───────────────────────────────────────────
CREATE TABLE dim.date (
    date_key    INT  NOT NULL,
    full_date   DATE NOT NULL,
    year        INT,  quarter INT, month INT,
    month_name  VARCHAR(20), week INT,
    day_of_week INT,  day_name VARCHAR(20), is_weekday BIT
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);
GO

-- ── FACT: Annotation Events (grain: 1 row per annotation record) ──────────
CREATE TABLE fact.annotation_events (
    annotation_key      BIGINT        NOT NULL,
    annotation_id       VARCHAR(200)  NOT NULL,
    annotator_key       INT,
    project_key         INT,
    date_key            INT,
    frame_id            VARCHAR(200),
    object_class        VARCHAR(200),
    label_category      VARCHAR(100),
    bbox_x_min          FLOAT,
    bbox_x_max          FLOAT,
    bbox_y_min          FLOAT,
    bbox_y_max          FLOAT,
    bbox_area           FLOAT,
    confidence_score    FLOAT,
    quality_score       FLOAT,
    quality_tier        VARCHAR(30),
    z_score             FLOAT,
    is_statistical_outlier BIT        DEFAULT 0,
    annotation_tool     VARCHAR(100),
    annotated_at        DATETIME2,
    _quality_scored_at  DATETIME2,
    _processed_at       DATETIME2
)
WITH (DISTRIBUTION = HASH(annotator_key), CLUSTERED COLUMNSTORE INDEX);
GO

-- ── FACT: Daily Annotator Quality Summary ─────────────────────
CREATE TABLE fact.daily_annotator_quality (
    summary_key             BIGINT    NOT NULL,
    annotator_key           INT,
    project_key             INT,
    date_key                INT,
    total_annotations       INT,
    high_quality_count      INT,
    acceptable_count        INT,
    flagged_count           INT,
    rejected_count          INT,
    avg_quality_score       FLOAT,
    avg_confidence          FLOAT,
    rejection_rate          FLOAT,
    wow_quality_change_pct  FLOAT,     -- week-over-week change
    _created_at             DATETIME2
)
WITH (DISTRIBUTION = HASH(annotator_key), CLUSTERED COLUMNSTORE INDEX);
GO

-- ── FACT: Dataset Readiness ───────────────────────────────────
CREATE TABLE fact.dataset_readiness (
    readiness_key           BIGINT    NOT NULL,
    project_key             INT,
    date_key                INT,
    total_frames            INT,
    annotated_frames        INT,
    qa_passed_frames        INT,
    annotation_coverage_pct FLOAT,
    qa_pass_rate_pct        FLOAT,
    ready_for_training      BIT,
    _calculated_at          DATETIME2
)
WITH (DISTRIBUTION = HASH(project_key), CLUSTERED COLUMNSTORE INDEX);
GO

-- ── STATISTICS ────────────────────────────────────────────────
CREATE STATISTICS s_ann_annotator   ON fact.annotation_events (annotator_key);
CREATE STATISTICS s_ann_project     ON fact.annotation_events (project_key);
CREATE STATISTICS s_ann_date        ON fact.annotation_events (date_key);
CREATE STATISTICS s_ann_quality     ON fact.annotation_events (quality_score);
CREATE STATISTICS s_ann_tier        ON fact.annotation_events (quality_tier);
GO
