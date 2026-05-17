-- ============================================================
-- File: watermark_control_table.sql
-- Purpose: Control table for watermark-based incremental loading
--          Stores last successfully processed timestamp per pipeline
-- ============================================================

CREATE SCHEMA IF NOT EXISTS watermark;
GO

CREATE TABLE watermark.pipeline_watermark (
    watermark_id        INT           IDENTITY(1,1) PRIMARY KEY,
    pipeline_name       VARCHAR(200)  NOT NULL UNIQUE,
    last_processed_ts   DATETIME2     NOT NULL DEFAULT '1900-01-01 00:00:00',
    last_updated        DATETIME2     DEFAULT GETDATE(),
    last_run_status     VARCHAR(50)   DEFAULT 'INITIALIZED',
    records_processed   BIGINT        DEFAULT 0,
    notes               VARCHAR(500)
);
GO

-- Seed initial watermarks for each pipeline
INSERT INTO watermark.pipeline_watermark (pipeline_name, last_processed_ts, notes)
VALUES
    ('annotation_ingestion',        '1900-01-01', 'Full load on first run'),
    ('model_feedback_ingestion',    '1900-01-01', 'Full load on first run'),
    ('image_metadata_ingestion',    '1900-01-01', 'Full load on first run'),
    ('annotator_profile_ingestion', '1900-01-01', 'Full load on first run');
GO

-- ── Usage Pattern (called from PySpark via JDBC MERGE) ────────────────────
-- After successful write, update the watermark:
--
-- MERGE INTO watermark.pipeline_watermark AS target
-- USING (SELECT 'annotation_ingestion' AS pipeline_name,
--               CAST('2024-11-15 14:30:00' AS DATETIME2) AS last_processed_ts) AS source
-- ON target.pipeline_name = source.pipeline_name
-- WHEN MATCHED THEN
--     UPDATE SET target.last_processed_ts = source.last_processed_ts,
--                target.last_updated = GETDATE(),
--                target.last_run_status = 'SUCCESS'
-- WHEN NOT MATCHED THEN
--     INSERT (pipeline_name, last_processed_ts, last_updated, last_run_status)
--     VALUES (source.pipeline_name, source.last_processed_ts, GETDATE(), 'SUCCESS');
-- GO

-- ── View: Current Watermark Status ────────────────────────────────────────
CREATE OR ALTER VIEW watermark.vw_watermark_status AS
SELECT
    pipeline_name,
    last_processed_ts,
    last_run_status,
    records_processed,
    last_updated,
    DATEDIFF(HOUR, last_processed_ts, GETDATE()) AS hours_since_last_load
FROM watermark.pipeline_watermark;
GO
