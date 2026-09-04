"""
Job Parameters:
    --JOB_NAME                                         — Glue job name (auto-set)
    --bronze_database      yt-pipeline-bronze-dev      — Bronze Glue catalog database
    --kaggle_table         raw_statistics              — Kaggle statistics table
    --api_table            raw_statistics_c85dbe...    — API statistics table
    --silver_bucket        yt-data-pipeline-silver-e   — Silver S3 bucket
    --silver_database      yt-pipeline-silver-dev      — Silver Glue database
    --silver_table         clean_statistics            — Silver statistics table
    --silver_path                                      — Silver S3 path prefix
"""

import sys
from datetime import datetime

from awsglue.transforms import *
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
logger.info(f"Bronze Kaggle: {BRONZE_DB}.{API_TABLE}")
logger.info(f"Silver: {SILVER_DB}.{SILVER_TABLE} → {SILVER_PATH}")


# ── Step 1: Read from Bronze ────────────────────────────────────────────────

logger.info("Reading from Bronze catalog...")

# Include the regions currently configured for the pipeline.
# This allows Spark to perform partition pruning.
predicate = "region in ('ca', 'gb', 'us', 'in')"

# Read the Bronze table into a Glue DynamicFrame.
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=BRONZE_TABLE,
    transformation_ctx="datasource",#operation name
    push_down_predicate=predicate,#filtering partitions by predicate
)


df = datasource.toDF()

logger.info(f"Spark columns: {df.columns}")

df.printSchema()

initial_count = df.count()

logger.info(f"Bronze records read: {initial_count}")


# ── Handle empty input ──────────────────────────────────────────────────────

if initial_count == 0:

    logger.info("No records to process. Committing job.")

else:

    # ── Step 2: Schema Enforcement ──────────────────────────────────────────
    logger.info("Enforcing schema and casting types...")

    # Detect whether this is the YouTube API JSON format
    # or the original Kaggle CSV format.
    columns = set(df.columns)

    # ────────────────────────────────────────────────────────────────────────
    # YouTube API JSON FORMAT
    # ────────────────────────────────────────────────────────────────────────

    if "items" in columns:

        logger.info(
            "Detected YouTube API format — exploding items array..."
        )

        # One API response contains an array of videos.
        # explode() converts:
        #
        # items = [video1, video2, video3]
        #
        # into:
        #
        # video1
        # video2
        # video3
        #
        df = df.select(
            F.explode("items").alias("item"),
            "region",
        )

        # Convert the nested API structure into the standardized
        # statistics schema used by the Silver layer.
        df = df.select(
            F.col("item.id").cast(StringType()).alias("video_id"),

            # The API does not provide the historical Kaggle
            # trending_date field, so we use the ingestion date.
            F.current_date().cast(StringType()).alias("trending_date"),

            F.col("item.snippet.title")
                .cast(StringType())
                .alias("title"),

            F.col("item.snippet.channelTitle")
                .cast(StringType())
                .alias("channel_title"),

            F.col("item.snippet.categoryId")
                .cast(LongType())
                .alias("category_id"),

            F.col("item.snippet.publishedAt")
                .cast(StringType())
                .alias("publish_time"),

            # YouTube API returns tags as an array.
            # Convert it into a single string for compatibility
            # with the existing Silver schema.
            F.concat_ws(
                ", ",
                F.col("item.snippet.tags"),
            ).alias("tags"),

            F.col("item.statistics.viewCount")
                .cast(LongType())
                .alias("views"),

            F.col("item.statistics.likeCount")
                .cast(LongType())
                .alias("likes"),

            # The YouTube API does not provide dislikes.
            F.lit(0)
                .cast(LongType())
                .alias("dislikes"),

            F.col("item.statistics.commentCount")
                .cast(LongType())
                .alias("comment_count"),

            F.col("item.snippet.thumbnails.default.url")
                .cast(StringType())
                .alias("thumbnail_link"),

            # These fields are not directly provided by this API response.
            F.lit(False).cast(BooleanType()).alias("comments_disabled"),

            F.lit(False).cast(BooleanType()).alias("ratings_disabled"),

            F.lit(False)
                .cast(BooleanType())
                .alias("video_error_or_removed"),

            F.col("item.snippet.description")
                .cast(StringType())
                .alias("description"),

            F.col("region")
                .cast(StringType())
                .alias("region"),
        )

    # ────────────────────────────────────────────────────────────────────────
    # KAGGLE CSV FORMAT
    # ────────────────────────────────────────────────────────────────────────

    else:

        logger.info(
            "Detected Kaggle CSV format — casting types..."
        )

        df = df.select(
            F.col("video_id").cast(StringType()),
            F.col("trending_date").cast(StringType()),
            F.col("title").cast(StringType()),
            F.col("channel_title").cast(StringType()),
            F.col("category_id").cast(LongType()),
            F.col("publish_time").cast(StringType()),
            F.col("tags").cast(StringType()),
            F.col("views").cast(LongType()),
            F.col("likes").cast(LongType()),
            F.col("dislikes").cast(LongType()),
            F.col("comment_count").cast(LongType()),
            F.col("thumbnail_link").cast(StringType()),
            F.col("comments_disabled").cast(BooleanType()),
            F.col("ratings_disabled").cast(BooleanType()),
            F.col("video_error_or_removed").cast(BooleanType()),
            F.col("description").cast(StringType()),
            F.col("region").cast(StringType()),
        )


    # ── Step 3: Data Cleansing ──────────────────────────────────────────────
    #
    # IMPORTANT:
    # This section is intentionally OUTSIDE the API/CSV if/else.
    #
    # At this point both input formats have been converted into
    # the same standardized schema.
    #
    # Therefore the remaining Silver transformations can be
    # applied to both formats.
    # ────────────────────────────────────────────────────────────────────────

    logger.info("Cleansing data...")


    # Remove records where video_id is null.
    df = df.filter(
        F.col("video_id").isNotNull()
    )


    # Standardize region codes to lowercase.
    df = df.withColumn(
        "region",
        F.lower(
            F.trim(
                F.col("region")
            )
        )
    )


    # ── Parse trending_date ─────────────────────────────────────────────────

    # Kaggle uses YY.DD.MM.
    #
    # API data has already been assigned an ingestion date above.
    #
    # Try the Kaggle format first, otherwise attempt normal date parsing.

    df = df.withColumn(
        "trending_date_parsed",

        #reg expression to check if the trending_date is in the format of YY.DD.MM
        F.when(
            F.col("trending_date").rlike(
                r"^\d{2}\.\d{2}\.\d{2}$"  
            ),

            F.to_date(
                F.col("trending_date"),
                "yy.dd.MM",
            ),
        #if not then treat it as a normal date format
        ).otherwise(

            F.to_date(
                F.col("trending_date")
            )
        )
    )


    # ── Fill null numeric values ────────────────────────────────────────────

    numeric_cols = [
        "views",
        "likes",
        "dislikes",
        "comment_count",
    ]

    for col_name in numeric_cols:

        df = df.withColumn(
            col_name,
            F.coalesce(
                F.col(col_name),
                F.lit(0),
            )
        )


    # ── Derived metrics ─────────────────────────────────────────────────────

    # Like ratio:
    #
    # likes / views * 100

    df = df.withColumn(
        "like_ratio",

        F.when(
            F.col("views") > 0,

            F.round(
                F.col("likes")
                / F.col("views")
                * 100,
                4,
            ),

        ).otherwise(0.0)
    )


    # Engagement rate:
    #
    # (likes + dislikes + comments) / views * 100

    df = df.withColumn(
        "engagement_rate",

        F.when(
            F.col("views") > 0,

            F.round(
                (
                    F.col("likes")
                    + F.col("dislikes")
                    + F.col("comment_count")
                )
                / F.col("views")
                * 100,
                4,
            ),

        ).otherwise(0.0)
    )


    # ── Processing metadata ─────────────────────────────────────────────────

    df = df.withColumn(
        "_processed_at",
        F.current_timestamp(),
    )

    df = df.withColumn(
        "_job_name",
        F.lit(args["JOB_NAME"]),
    )


    # ── Step 4: Deduplication ───────────────────────────────────────────────

    logger.info("Deduplicating...")


    # Keep the latest record per:
    #
    # video_id + region + trending_date_parsed
    #
    window = (
        Window
        .partitionBy(
            "video_id",
            "region",
            "trending_date_parsed",
        )
        .orderBy(
            F.col("_processed_at").desc()
        )
    )


    df = (
        df
        .withColumn(
            "_row_num",
            F.row_number().over(window),
        )
        .filter(
            F.col("_row_num") == 1 #filter most recent entry then delete _row_num col since we dont need it moving forward
        )
        .drop("_row_num")
    )


    clean_count = df.count()

    logger.info(
        f"After cleansing & dedup: "
        f"{clean_count} records "
        f"(removed {initial_count - clean_count})"
    )


    # ── Step 5: Data Quality Checks ─────────────────────────────────────────

    logger.info("Running data quality checks...")

    #dictionary to keep track of how many nulls we find in certain important columns
    null_counts = {}


    for col_name in [
        "video_id",
        "title",
        "channel_title",
        "views",
    ]:

        null_count = (
            df
            .filter(
                F.col(col_name).isNull()
            )
            .count()
        )

        null_counts[col_name] = null_count

        if null_count > 0:

            logger.warn(
                f"DQ WARNING: "
                f"{col_name} has "
                f"{null_count} null values"
            )


    # Check for negative views.
    negative_views = (
        df
        .filter(
            F.col("views") < 0
        )
        .count()
    )


    if negative_views > 0:

        logger.warn(
            f"DQ WARNING: "
            f"{negative_views} records "
            f"with negative views"
        )

    #final DQ log
    logger.info(
        f"DQ check complete. "
        f"Null counts: {null_counts}"
    )


    # ── Step 6: Write to Silver Layer ───────────────────────────────────────

    logger.info(
        f"Writing to Silver: {SILVER_PATH}"
    )


    # Convert Spark DataFrame back into a Glue DynamicFrame.
    dynamic_frame = DynamicFrame.fromDF(
        df,
        glueContext,
        "silver_statistics",
    )


    # Configure the S3 sink.
    # Create an output destination, and that destination is S3.
    sink = glueContext.getSink(
        connection_type="s3",
        path=SILVER_PATH,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE", #Update the existing Glue Catalog table with the schema/information associated with this write
        partitionKeys=["region"],
    )


    # Tell Glue which Data Catalog table should be updated.
    sink.setCatalogInfo(
        catalogDatabase=SILVER_DB,
        catalogTableName=SILVER_TABLE,
    )


    # Write Parquet with Snappy compression.
    sink.setFormat(
        "glueparquet",
        compression="snappy",
    )


    # Perform the actual Silver write.
    sink.writeFrame(dynamic_frame)


    logger.info(
        f"Silver write complete. "
        f"{clean_count} records written."
    )


# ── Commit Glue Job ──────────────────────────────────────────────────────────

job.commit() #close glue job

logger.info("Glue job committed successfully.")
