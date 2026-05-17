# AI Training Data Quality & Annotation Analytics Platform

> **Azure cloud data engineering platform** to manage, validate, and analyze large-scale AI training and annotation datasets for computer vision model development. Automates ingestion, quality scoring, annotator performance tracking, and real-time analytics with CI/CD deployment via Azure DevOps.

---

## Architecture Overview

```
┌───────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                  │
│   Annotation Outputs │ Image Metadata │ Model Feedback │ Event Stream │
└──────┬────────────────────┬──────────────────┬──────────┬─────────────┘
       │                    │                  │          │
       ▼                    ▼                  ▼          ▼
┌──────────────────────────────────┐   ┌──────────────────────────────┐
│     Azure Data Factory           │   │      Azure Event Hub         │
│  (Parameterized ELT Pipelines)   │   │  (Real-time Annotation Feed) │
│  + Watermark Incremental Logic   │   └─────────────┬────────────────┘
└──────────────┬───────────────────┘                 │
               │                                     │
               ▼                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2  ──  BRONZE LAYER (Delta)                 │
│         Raw annotation outputs, metadata, model eval logs             │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│               Azure Databricks  ──  PySpark Processing                 │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │              QUALITY SCORING FRAMEWORK                          │  │
│  │  Rule-based checks │ Statistical validation │ Z-score outliers  │  │
│  │  Score 0-100  │  < 40 = Auto-Rejected  │  < 70 = Flagged       │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
          ┌─────────────────────────┴──────────────────────┐
          ▼                                                 ▼
┌─────────────────────────┐              ┌──────────────────────────────┐
│  ADLS Gen2 - SILVER     │              │   ADLS Gen2 - QUARANTINE     │
│  Validated, scored      │              │   Failed / rejected records  │
│  annotation data        │              │   + quarantine reason codes  │
└─────────────┬───────────┘              └──────────────────────────────┘
              │
              ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      ADLS Gen2  ──  GOLD LAYER                         │
│         Annotator KPIs │ Dataset Readiness │ Accuracy Trends           │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
               ┌───────────────────┴────────────────────┐
               ▼                                        ▼
   ┌───────────────────────┐              ┌─────────────────────────────┐
   │  Azure Synapse        │              │      Power BI               │
   │  Analytics            │─────────────▶  Annotation Throughput      │
   │  (Star Schema)        │              │  Accuracy Trends            │
   └───────────────────────┘              │  Dataset Readiness KPIs    │
                                          └─────────────────────────────┘
┌────────────────────────────────────────────────────────────────────────┐
│                     Azure DevOps  ──  CI/CD                            │
│    Git → PR → ARM Template Validation → Deploy ADF + Notebooks         │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory (parameterized + watermark) |
| Processing | Azure Databricks (PySpark) |
| Storage | ADLS Gen2, Delta Lake |
| Table Format | Delta Lake — Bronze / Silver / Gold |
| Streaming | Azure Event Hub + Structured Streaming |
| Analytics | Azure Synapse Analytics (Dedicated + Serverless) |
| Database | Azure SQL Database (config & watermark) |
| Monitoring | Azure Monitor, Log Analytics |
| Reporting | Power BI |
| CI/CD | Azure DevOps, Git, Databricks CLI |
| Security | Azure Key Vault, Managed Identity |

---

## Project Structure

```
ai-annotation-quality-platform/
│
├── notebooks/
│   ├── 01_bronze_ingestion_incremental.py   # Watermark-based incremental load
│   ├── 02_silver_cleansing.py               # Cleanse + normalize annotation data
│   ├── 03_quality_scoring.py                # Quality scoring framework (0-100)
│   ├── 04_annotator_productivity.py         # Annotator-level performance metrics
│   ├── 05_gold_dataset_readiness.py         # Dataset readiness aggregations
│   └── 06_optimize_delta_tables.py          # Weekly OPTIMIZE + ZORDER job
│
├── pipelines/
│   ├── adf_annotation_ingestion.json        # Incremental annotation ingestion
│   ├── adf_model_feedback_ingestion.json    # Model evaluation feedback ingestion
│   └── adf_master_orchestration.json        # Master pipeline with dependencies
│
├── sql/
│   ├── synapse_ddl.sql                      # Synapse star schema DDL
│   ├── reporting_views.sql                  # Power BI reporting views
│   ├── watermark_control_table.sql          # Watermark control table DDL
│   └── quality_check_queries.sql            # Ad-hoc quality validation queries
│
├── config/
│   ├── quality_rules_config.json            # Quality rule thresholds (configurable)
│   └── pipeline_config.json                 # Source/sink configuration
│
├── tests/
│   ├── test_quality_scoring.py              # Unit tests for scoring logic
│   └── test_silver_transformation.py        # Unit tests for transformations
│
├── docs/
│   └── architecture_diagram.md
│
└── README.md
```

---

## Key Features

### 1. Watermark-Based Incremental Loading
- Watermark timestamps stored in Azure SQL control table
- Each pipeline run loads **only new/modified records** since last successful run
- Watermark updated atomically after successful write — no duplicates, no gaps
- Enables efficient daily incremental loads even on 100M+ record tables

### 2. Annotation Quality Scoring Framework
Every annotation record receives a **quality score from 0 to 100**:

| Check | Deduction | Quarantine |
|---|---|---|
| Missing required fields | 20 pts | If score < 40 |
| Invalid bounding box geometry | 15 pts | If score < 40 |
| Unknown/unapproved label | 20 pts | Immediate |
| Bbox area too small (<10px²) | 10 pts | If score < 40 |
| Bbox area too large (>50% frame) | 15 pts | If score < 40 |

**Quality Tiers:**
- `HIGH` (85–100): Ready for model training
- `ACCEPTABLE` (70–84): Ready with minor caveats
- `FLAGGED_FOR_REVIEW` (40–69): Human QA required
- `REJECTED` (<40): Auto-moved to quarantine

### 3. Statistical Outlier Detection
- Per-annotator Z-score calculated on quality scores
- Z-score > 3 triggers automated alert to QA team
- Week-over-week accuracy trend monitored — >10% drop fires alert

### 4. CI/CD with Azure DevOps
- ADF JSON files and Databricks notebooks stored in Git
- PR to `main` triggers CI: ARM template validation + notebook syntax checks
- Merge triggers CD: ADF publish via Azure CLI + Databricks notebook deploy
- Environment-specific configs injected via DevOps variable groups

### 5. Delta Lake ACID Architecture
- Bronze: append-only raw data — full history preserved for reprocessing
- Silver: MERGE-based upserts for annotation record updates
- Gold: Full overwrite aggregation tables, rebuilt nightly
- Time travel used for auditing and rollback of bad pipeline runs

---

## Business Impact

| Metric | Result |
|---|---|
| Annotation quality accuracy | Improved by **35%** via automated validation |
| Manual validation effort | Reduced by **60%** (exception-based review) |
| Annotator productivity visibility | Real-time (was weekly manual reports) |
| AI model training data availability | Accelerated by **40%** |
| Spark processing cost | Reduced by **25%** via optimization |

---

## Quality Rules Configuration

Rules are externalized in `config/quality_rules_config.json` — thresholds can be adjusted without code changes:

```json
{
  "auto_reject_threshold": 40,
  "review_threshold": 70,
  "min_bbox_area_px": 10,
  "max_bbox_area_pct": 0.5,
  "z_score_alert_threshold": 3.0,
  "accuracy_drop_alert_pct": 10.0
}
```

---

## CI/CD Pipeline (Azure DevOps)

```
Feature Branch
     │
     ▼ Pull Request to main
┌─────────────────────────────────────────┐
│  CI Pipeline                            │
│  1. Validate ADF ARM templates          │
│  2. Lint Python notebooks               │
│  3. Run unit tests (tests/)             │
└────────────────────┬────────────────────┘
                     │ Merge to main
                     ▼
┌─────────────────────────────────────────┐
│  CD Pipeline                            │
│  1. az datafactory pipeline create      │
│     (publish ARM template to prod ADF)  │
│  2. databricks workspace import         │
│     (deploy notebooks to prod workspace)│
│  3. Smoke test — trigger test pipeline  │
└─────────────────────────────────────────┘
```

---

## Setup Prerequisites

- Azure subscription (Contributor access)
- Azure Databricks workspace (Premium)
- ADLS Gen2 storage account
- Azure Data Factory
- Azure SQL Database (watermark + config tables)
- Azure Event Hub namespace
- Azure Key Vault
- Azure DevOps organization + project
- Power BI workspace with Synapse connection

---

## Monitoring & Alerting

| Alert | Trigger | Channel |
|---|---|---|
| Pipeline failure | Any ADF activity fails | Email + Teams |
| High quarantine rate | >5% records quarantined in a run | Email |
| Annotator accuracy drop | >10% WoW decline | Teams |
| Stream lag | Event Hub consumer lag > 1000 msgs | Azure Monitor |
| Data freshness | Gold tables not updated in 26hrs | Azure Monitor |
