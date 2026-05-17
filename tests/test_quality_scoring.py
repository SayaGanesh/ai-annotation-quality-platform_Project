"""
File: tests/test_quality_scoring.py
Project: AI Training Data Quality & Annotation Analytics Platform
Purpose: Unit tests for annotation quality scoring logic
Run with: pytest tests/test_quality_scoring.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_quality_scoring")
        .getOrCreate()
    )


@pytest.fixture
def sample_annotations(spark):
    """Create sample annotation DataFrame for testing."""
    schema = StructType([
        StructField("annotation_id",   StringType(), True),
        StructField("annotator_id",    StringType(), True),
        StructField("object_class",    StringType(), True),
        StructField("label_id",        IntegerType(), True),
        StructField("bbox_x_min",      FloatType(), True),
        StructField("bbox_x_max",      FloatType(), True),
        StructField("bbox_y_min",      FloatType(), True),
        StructField("bbox_y_max",      FloatType(), True),
        StructField("confidence_score", FloatType(), True),
        StructField("frame_id",        StringType(), True),
    ])
    data = [
        # Valid annotation — should score HIGH
        ("ann_001", "user_1", "car",    1,  100.0, 300.0, 100.0, 250.0, 0.95, "frame_001"),
        # Missing confidence — 10pt deduction
        ("ann_002", "user_1", "truck",  2,  200.0, 400.0, 150.0, 350.0, None, "frame_002"),
        # Invalid bbox x (x_max < x_min) — should have been caught in Silver; score low
        ("ann_003", "user_2", "person", 3,  400.0, 200.0, 100.0, 300.0, 0.80, "frame_003"),
        # Unknown label (label_id is null) — -20pts
        ("ann_004", "user_2", "alien",  None, 50.0, 150.0,  50.0, 150.0, 0.70, "frame_004"),
        # Tiny bbox area (5px²) — -10pts
        ("ann_005", "user_3", "car",    1,  100.0, 102.0, 100.0, 102.5, 0.90, "frame_005"),
        # Missing object_class — -20pts
        ("ann_006", "user_3", None,     None, 100.0, 300.0, 100.0, 250.0, 0.85, "frame_006"),
    ]
    return spark.createDataFrame(data, schema)


def apply_quality_scoring(df):
    """Inline quality scoring logic for test isolation."""
    from pyspark.sql.functions import when, lit, round as spark_round

    IMAGE_WIDTH  = 1920.0
    IMAGE_HEIGHT = 1080.0
    MIN_BBOX_AREA = 10.0
    MAX_BBOX_AREA_PCT = 0.5
    AUTO_REJECT = 40
    REVIEW_THRESH = 70

    return (df
        .withColumn("bbox_area",
            (col("bbox_x_max") - col("bbox_x_min")) *
            (col("bbox_y_max") - col("bbox_y_min"))
        )
        .withColumn("bbox_area_pct",
            col("bbox_area") / (IMAGE_WIDTH * IMAGE_HEIGHT)
        )
        .withColumn("d_missing_confidence",
            when(col("confidence_score").isNull(), 10).otherwise(0)
        )
        .withColumn("d_small_bbox",
            when(col("bbox_area") < MIN_BBOX_AREA, 10).otherwise(0)
        )
        .withColumn("d_unknown_label",
            when(col("label_id").isNull(), 20).otherwise(0)
        )
        .withColumn("d_missing_fields",
            when(col("object_class").isNull(), 20).otherwise(0)
        )
        .withColumn("quality_score",
            spark_round(
                100
                - col("d_missing_confidence")
                - col("d_small_bbox")
                - col("d_unknown_label")
                - col("d_missing_fields"),
                2
            )
        )
        .withColumn("quality_tier",
            when(col("quality_score") >= 85, "HIGH")
            .when(col("quality_score") >= REVIEW_THRESH, "ACCEPTABLE")
            .when(col("quality_score") >= AUTO_REJECT, "FLAGGED_FOR_REVIEW")
            .otherwise("REJECTED")
        )
    )


class TestQualityScoring:

    def test_valid_annotation_scores_high(self, spark, sample_annotations):
        scored = apply_quality_scoring(sample_annotations)
        ann_001 = scored.filter(col("annotation_id") == "ann_001").first()
        assert ann_001["quality_score"] == 100.0
        assert ann_001["quality_tier"] == "HIGH"

    def test_missing_confidence_deducts_10(self, spark, sample_annotations):
        scored = apply_quality_scoring(sample_annotations)
        ann_002 = scored.filter(col("annotation_id") == "ann_002").first()
        assert ann_002["quality_score"] == 90.0

    def test_unknown_label_deducts_20(self, spark, sample_annotations):
        scored = apply_quality_scoring(sample_annotations)
        ann_004 = scored.filter(col("annotation_id") == "ann_004").first()
        assert ann_004["quality_score"] == 80.0  # 100 - 20 (unknown label)

    def test_tiny_bbox_deducts_10(self, spark, sample_annotations):
        scored = apply_quality_scoring(sample_annotations)
        ann_005 = scored.filter(col("annotation_id") == "ann_005").first()
        # bbox area = 2 * 2.5 = 5px² < 10 threshold
        assert ann_005["d_small_bbox"] == 10

    def test_missing_object_class_flagged(self, spark, sample_annotations):
        scored = apply_quality_scoring(sample_annotations)
        ann_006 = scored.filter(col("annotation_id") == "ann_006").first()
        # missing class (-20) + unknown label (-20) = 60
        assert ann_006["quality_score"] == 60.0
        assert ann_006["quality_tier"] == "FLAGGED_FOR_REVIEW"

    def test_no_records_lost(self, spark, sample_annotations):
        scored = apply_quality_scoring(sample_annotations)
        assert scored.count() == sample_annotations.count()

    def test_all_tiers_assigned(self, spark, sample_annotations):
        scored = apply_quality_scoring(sample_annotations)
        tiers = [row["quality_tier"] for row in scored.select("quality_tier").collect()]
        assert all(t in ["HIGH", "ACCEPTABLE", "FLAGGED_FOR_REVIEW", "REJECTED"]
                   for t in tiers)

    def test_quality_score_within_bounds(self, spark, sample_annotations):
        scored = apply_quality_scoring(sample_annotations)
        out_of_range = scored.filter(
            (col("quality_score") < 0) | (col("quality_score") > 100)
        )
        assert out_of_range.count() == 0, "Quality scores must be between 0 and 100"
