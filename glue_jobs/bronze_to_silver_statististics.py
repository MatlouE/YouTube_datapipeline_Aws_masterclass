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


# ── Step 1: Read from Bronze Kaggle table ────────────────────────────────────────────────

logger.info("Reading from Bronze Kaggle catalog...")

kaggle_source = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=KAGGLE_TABLE,
    transformation_ctx="kaggle_source",
)

kaggle_df = kaggle_source.toDF()

kaggle_count = kaggle_df.count()

logger.info(f"Kaggle records read from Bronze: {kaggle_count}")


# ── Step 2: Read from Bronze API table ────────────────────────────────────────────────

api_source = glueContext.create_dynamic_frame.from_catalog(
    database=BRONZE_DB,
    table_name=API_TABLE,
    transformation_ctx="api_source",
)

api_df = api_source.toDF()

api_count = api_df.count()

logger.info(f"API records read from Bronze: {api_count}")



    
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
