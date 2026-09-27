"""
YouTube Data Pipeline — Bronze to Silver Statistics

Job Parameters:
    --JOB_NAME
    --bronze_database      yt-pipeline-bronze-dev
    --kaggle_table         raw_statistics
    --api_table            raw_statistics_c85dbe...
    --silver_bucket        yt-data-pipeline-silver-e
    --silver_database      yt-pipeline-silver-dev
    --silver_table         clean_statistics

Pipeline:
    Bronze Kaggle CSV
        +
    Bronze YouTube API JSON
        ↓
    Source-specific normalization
        ↓
    Common Silver schema
        ↓
    Data quality checks
        ↓
    Cleaning / standardization
        ↓
    Engagement metrics
        ↓
    Deterministic deduplication
        ↓
    Silver Parquet
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    LongType,
    BooleanType,
)
from pyspark.sql.window import Window


# ── Job Setup ────────────────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bronze_database",
        "kaggle_table",
        "api_table",
        "silver_bucket",
        "silver_database",
        "silver_table",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()


# ── Config ───────────────────────────────────────────────────────────────────

BRONZE_DB = args["bronze_database"]
KAGGLE_TABLE = args["kaggle_table"]
API_TABLE = args["api_table"]

SILVER_BUCKET = args["silver_bucket"]
SILVER_DB = args["silver_database"]
SILVER_TABLE = args["silver_table"]

SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/statistics/"


logger.info(f"Bronze Kaggle: {BRONZE_DB}.{KAGGLE_TABLE}")
logger.info(f"Bronze API: {BRONZE_DB}.{API_TABLE}")
logger.info(f"Silver: {SILVER_DB}.{SILVER_TABLE} → {SILVER_PATH}")


# ── Step 1: Read Kaggle Bronze ──────────────────────────────────────────────

logger.info("Reading from Bronze Kaggle catalog...")

kaggle_source = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=KAGGLE_TABLE,
    transformation_ctx="kaggle_source",
)

kaggle_df = kaggle_source.toDF()

kaggle_count = kaggle_df.count()

logger.info(f"Kaggle records read from Bronze: {kaggle_count}")

logger.info("Kaggle Bronze schema:")
kaggle_df.printSchema()


# ── Step 2: Read YouTube API Bronze ─────────────────────────────────────────

logger.info("Reading from Bronze API catalog...")

api_source = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=API_TABLE,
    transformation_ctx="api_source",
)

api_df = api_source.toDF()

api_count = api_df.count()

logger.info(f"API records read from Bronze: {api_count}")

logger.info("API Bronze schema:")
api_df.printSchema()


# ── Step 3: Flatten and Normalize API Data ──────────────────────────────────

logger.info("Flattening API items array...")

api_normalized_df = (
    api_df
    .withColumn(
        "item",
        F.explode(F.col("items"))
    )
    .select(
        F.col("item.id").alias("video_id"),

        F.col("item.snippet.title").alias("title"),

        F.col("item.snippet.channelTitle").alias("channel_title"),

        F.col("item.snippet.categoryId")
        .cast(LongType())
        .alias("category_id"),

        F.col("item.snippet.publishedAt")
        .alias("publish_time"),

        # API does not provide the Kaggle tags field
        F.lit(None)
        .cast(StringType())
        .alias("tags"),

        F.col("item.statistics.viewCount")
        .cast(LongType())
        .alias("views"),

        F.col("item.statistics.likeCount")
        .cast(LongType())
        .alias("likes"),

        # API does not provide dislikes
        F.lit(0)
        .cast(LongType())
        .alias("dislikes"),

        F.col("item.statistics.commentCount")
        .cast(LongType())
        .alias("comment_count"),

        # API does not populate the Kaggle thumbnail_link field
        F.lit(None)
        .cast(StringType())
        .alias("thumbnail_link"),

        # Kaggle-specific boolean fields
        F.lit(False)
        .cast(BooleanType())
        .alias("comments_disabled"),

        F.lit(False)
        .cast(BooleanType())
        .alias("ratings_disabled"),

        F.lit(False)
        .cast(BooleanType())
        .alias("video_error_or_removed"),

        F.col("item.snippet.description")
        .alias("description"),

        # Metadata created by the API ingestion
        F.col("region")
        .alias("region"),

        F.col("date")
        .alias("trending_date"),
    )
)

logger.info("API data flattened and normalized.")


# ── Step 4: Normalize Kaggle Data ───────────────────────────────────────────

logger.info("Normalizing Kaggle Bronze data...")

kaggle_normalized_df = (
    kaggle_df
    .select(
        F.col("video_id"),

        F.col("title"),

        F.col("channel_title"),

        F.col("category_id")
        .cast(LongType())
        .alias("category_id"),

        F.col("publish_time"),

        F.col("tags"),

        F.col("views")
        .cast(LongType())
        .alias("views"),

        F.col("likes")
        .cast(LongType())
        .alias("likes"),

        F.col("dislikes")
        .cast(LongType())
        .alias("dislikes"),

        F.col("comment_count")
        .cast(LongType())
        .alias("comment_count"),

        F.col("thumbnail_link"),

        F.col("comments_disabled")
        .cast(BooleanType())
        .alias("comments_disabled"),

        F.col("ratings_disabled")
        .cast(BooleanType())
        .alias("ratings_disabled"),

        F.col("video_error_or_removed")
        .cast(BooleanType())
        .alias("video_error_or_removed"),

        F.col("description"),

        F.col("region"),

        F.col("trending_date"),
    )
)

logger.info("Kaggle data normalized.")


# ── Step 5: Add Internal Source Priority ────────────────────────────────────
#
# This is an internal technical field.
# It will NOT be written to Silver.
#
# 1 = Kaggle
# 2 = YouTube API
#
# If the same logical observation exists in both sources,
# the API record will be retained.

logger.info("Adding internal source priority...")

kaggle_normalized_df = kaggle_normalized_df.withColumn(
    "_source_priority",
    F.lit(1)
)

api_normalized_df = api_normalized_df.withColumn(
    "_source_priority",
    F.lit(2)
)


# ── Step 6: Combine Sources ─────────────────────────────────────────────────

logger.info("Combining Kaggle and API datasets...")

df = kaggle_normalized_df.unionByName(
    api_normalized_df
)

combined_count = df.count()

logger.info(
    f"Combined Bronze records: {combined_count}"
)


# ── Step 7: Initial Data Quality Checks ─────────────────────────────────────
#
# These checks happen BEFORE destructive cleaning such as:
#   - filtering null video IDs
#   - replacing null metrics with zero
#
# This ensures the measurements actually tell us something about
# the incoming Bronze data.

logger.info("Running pre-cleaning data quality checks...")

null_video_ids = df.filter(
    F.col("video_id").isNull()
).count()

null_titles = df.filter(
    F.col("title").isNull()
).count()

null_channels = df.filter(
    F.col("channel_title").isNull()
).count()

null_views = df.filter(
    F.col("views").isNull()
).count()

negative_views = df.filter(
    F.col("views") < 0
).count()

logger.info(
    f"Pre-cleaning quality results — "
    f"null video_id: {null_video_ids}, "
    f"null title: {null_titles}, "
    f"null channel_title: {null_channels}, "
    f"null views: {null_views}, "
    f"negative views: {negative_views}"
)

if null_video_ids > 0:
    logger.warning(
        f"Found {null_video_ids} records with null video IDs. "
        "These records will be removed."
    )

if negative_views > 0:
    logger.warning(
        f"Found {negative_views} records with negative view counts."
    )


# ── Step 8: Clean and Standardize ──────────────────────────────────────────

logger.info("Cleaning and standardizing combined data...")


# Remove records that cannot be identified as a video.
df = df.filter(
    F.col("video_id").isNotNull()
)


# Normalize region values.
df = df.withColumn(
    "region",
    F.lower(
        F.trim(
            F.col("region")
        )
    )
)


# Normalize trending date.
#
# Keep the original normalized trending_date column as the Silver
# business field. A temporary parsed date is created for validation
# and deduplication.

df = df.withColumn(
    "trending_date_parsed",
    F.to_date(
        F.col("trending_date")
    )
)


# Replace null numeric metrics with zero.
numeric_columns = [
    "views",
    "likes",
    "dislikes",
    "comment_count",
]

for column in numeric_columns:
    df = df.withColumn(
        column,
        F.coalesce(
            F.col(column),
            F.lit(0).cast(LongType())
        )
    )


logger.info("Data cleansing and standardization complete.")


# ── Step 9: Validate Parsed Dates ───────────────────────────────────────────

logger.info("Checking trending date parsing...")

invalid_trending_dates = df.filter(
    F.col("trending_date").isNotNull()
    & F.col("trending_date_parsed").isNull()
).count()

logger.info(
    f"Invalid trending dates: {invalid_trending_dates}"
)

if invalid_trending_dates > 0:
    logger.warning(
        f"Found {invalid_trending_dates} records with invalid "
        "trending dates."
    )


# ── Step 10: Calculate Engagement Metrics ───────────────────────────────────

logger.info("Calculating engagement metrics...")


df = df.withColumn(
    "like_ratio",
    F.when(
        F.col("views") > 0,
        F.round(
            (
                F.col("likes")
                / F.col("views")
            ) * 100,
            4
        )
    ).otherwise(0.0)
)


df = df.withColumn(
    "engagement_rate",
    F.when(
        F.col("views") > 0,
        F.round(
            (
                F.col("likes")
                + F.col("dislikes")
                + F.col("comment_count")
            ) / F.col("views") * 100,
            4
        )
    ).otherwise(0.0)
)


logger.info("Engagement metrics calculated.")


# ── Step 11: Add Processing Metadata ────────────────────────────────────────

logger.info("Adding processing metadata...")


df = df.withColumn(
    "_processed_at",
    F.current_timestamp()
)


df = df.withColumn(
    "_job_name",
    F.lit(args["JOB_NAME"])
)


logger.info("Processing metadata added.")


# ── Step 12: Deduplicate ────────────────────────────────────────────────────
#
# Business grain:
#
#     video_id + region + trending_date
#
# If both Kaggle and API contain the same logical observation,
# API takes priority through _source_priority.
#
# _source_priority is removed before the final Silver write.

logger.info("Deduplicating combined data...")

dedup_window = Window.partitionBy(
    "video_id",
    "region",
    "trending_date"
).orderBy(
    F.col("_source_priority").desc()
)


df = (
    df
    .withColumn(
        "_row_number",
        F.row_number().over(dedup_window)
    )
    .filter(
        F.col("_row_number") == 1
    )
    .drop("_row_number")
)


logger.info("Deduplication complete.")


# ── Step 13: Remove Temporary Columns ───────────────────────────────────────

logger.info("Removing temporary transformation columns...")

df = df.drop(
    "_source_priority",
    "trending_date_parsed"
)


logger.info("Temporary columns removed.")


# ── Step 14: Final Data Quality Checks ──────────────────────────────────────

logger.info("Running final Silver data quality checks...")

final_null_video_ids = df.filter(
    F.col("video_id").isNull()
).count()

final_negative_views = df.filter(
    F.col("views") < 0
).count()

final_duplicate_records = (
    df.groupBy(
        "video_id",
        "region",
        "trending_date"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


logger.info(
    f"Final quality results — "
    f"null video_id: {final_null_video_ids}, "
    f"negative views: {final_negative_views}, "
    f"duplicate business keys: {final_duplicate_records}"
)


if final_null_video_ids > 0:
    raise ValueError(
        "Final Silver dataset contains null video IDs."
    )


if final_negative_views > 0:
    raise ValueError(
        "Final Silver dataset contains negative view counts."
    )


if final_duplicate_records > 0:
    raise ValueError(
        "Final Silver dataset contains duplicate business keys."
    )


logger.info("Final Silver data quality checks passed.")


# ── Step 15: Finalize Silver Dataset ────────────────────────────────────────

logger.info("Finalizing Silver dataset...")

clean_count = df.count()

logger.info(
    f"Silver dataset ready: {clean_count} records"
)


logger.info("Final Silver schema:")
df.printSchema()


# ── Step 16: Write Silver Parquet ───────────────────────────────────────────

logger.info(
    f"Writing Silver dataset to: {SILVER_PATH}"
)


dynamic_frame = DynamicFrame.fromDF(
    df,
    glueContext,
    "silver_statistics"
)


sink = glueContext.getSink(
    connection_type="s3",
    path=SILVER_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)


sink.setCatalogInfo(
    catalogDatabase=SILVER_DB,
    catalogTableName=SILVER_TABLE,
)


sink.setFormat(
    "glueparquet",
    compression="snappy"
)


sink.writeFrame(dynamic_frame)


logger.info(
    f"Silver write complete. "
    f"{clean_count} records written."
)


# ── Step 17: Commit Glue Job ────────────────────────────────────────────────

job.commit()

logger.info(
    "Glue job committed successfully."
)