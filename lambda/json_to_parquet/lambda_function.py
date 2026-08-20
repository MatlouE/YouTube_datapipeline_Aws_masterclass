"""
Lambda: JSON Reference Data → Silver Layer (Parquet)

Triggered by S3 when a new JSON file lands in the Bronze bucket
under:

youtube/raw_statistics_reference_data/

Flow:

S3 Bronze JSON
      ↓
S3 Event Notification
      ↓
Lambda
      ↓
Read JSON
      ↓
Normalize category data
      ↓
Validate
      ↓
Add metadata
      ↓
Write Parquet
      ↓
S3 Silver
      ↓
Glue Data Catalog
"""

import json
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"].strip()

GLUE_DB = os.environ.get(
    "GLUE_DB_SILVER",
    "yt-pipeline-silver-dev"
).strip()

GLUE_TABLE = os.environ.get(
    "GLUE_TABLE_REFERENCE",
    "clean_reference_data"
).strip()

SNS_TOPIC = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    ""
).strip()

SILVER_PATH = (
    f"s3://{SILVER_BUCKET}/youtube/reference_data/"
)


# ─────────────────────────────────────────────────────────────────────────────
# AWS clients
# ─────────────────────────────────────────────────────────────────────────────

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


# ─────────────────────────────────────────────────────────────────────────────
# Read JSON from S3
# ─────────────────────────────────────────────────────────────────────────────

def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    Read JSON object from S3 and convert it into a Python dictionary.
    """

    logger.info(
        f"Reading JSON from s3://{bucket}/{key}"
    )

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    content = response["Body"].read().decode("utf-8")

    data = json.loads(content)

    logger.info(
        "Successfully read and parsed JSON from S3"
    )

    return data


# ─────────────────────────────────────────────────────────────────────────────
# Validate category data
# ─────────────────────────────────────────────────────────────────────────────

def validate_category_data(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Validate and clean category reference data.
    """

    logger.info(
        f"Validating DataFrame with {len(df)} rows"
    )

    if df.empty:
        raise ValueError(
            "Empty DataFrame — no category items found"
        )

    required_cols = {
        "id",
        "snippet.title"
    }

    actual_cols = set(df.columns)

    missing = required_cols - actual_cols

    if missing:
        logger.warning(
            f"Missing expected columns: {missing}"
        )

    # Remove duplicate category IDs
    if "id" in df.columns:

        before = len(df)

        df = df.drop_duplicates(
            subset=["id"],
            keep="last"
        )

        after = len(df)

        if before != after:
            logger.info(
                f"Removed {before - after} duplicate categories"
            )

    logger.info(
        "Validation completed successfully"
    )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# SNS alert
# ─────────────────────────────────────────────────────────────────────────────

def send_alert(
    subject: str,
    message: str
):
    """
    Send an SNS alert if an SNS topic is configured.
    """

    if not SNS_TOPIC:
        logger.info(
            "SNS topic not configured. Skipping alert."
        )
        return

    logger.info(
        f"Sending SNS alert: {subject}"
    )

    sns_client.publish(
        TopicArn=SNS_TOPIC,
        Subject=subject[:100],
        Message=message
    )


# ─────────────────────────────────────────────────────────────────────────────
# Lambda handler
# ─────────────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):

    logger.info(
        "========== Lambda execution started =========="
    )

    logger.info(
        f"Event received: {json.dumps(event)}"
    )

    logger.info(
        f"Silver bucket: '{SILVER_BUCKET}'"
    )

    logger.info(
        f"Silver path: '{SILVER_PATH}'"
    )

    logger.info(
        f"Glue database: '{GLUE_DB}'"
    )

    logger.info(
        f"Glue table: '{GLUE_TABLE}'"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Extract records from S3 event
    # ─────────────────────────────────────────────────────────────────────────

    records = event.get("Records", [])

    if not records:

        logger.warning(
            "No Records found in event."
        )

        # Allows a manually constructed event containing an S3 object
        # to still be processed.
        if "s3" in event:
            records = [event]

    logger.info(
        f"Number of records received: {len(records)}"
    )

    processed = []
    errors = []

    # ─────────────────────────────────────────────────────────────────────────
    # Process each S3 record
    # ─────────────────────────────────────────────────────────────────────────

    for record in records:

        key = "unknown"

        try:

            # ─────────────────────────────────────────────────────────────
            # Extract S3 information
            # ─────────────────────────────────────────────────────────────

            s3_info = record["s3"]

            bucket = s3_info["bucket"]["name"]

            key = unquote_plus(
                s3_info["object"]["key"]
            )

            logger.info(
                "------------------------------------------------"
            )

            logger.info(
                f"Processing bucket: {bucket}"
            )

            logger.info(
                f"Processing key: {key}"
            )

            # ─────────────────────────────────────────────────────────────
            # Read JSON
            # ─────────────────────────────────────────────────────────────

            raw_data = read_json_from_s3(
                bucket,
                key
            )

            logger.info(
                f"Top-level JSON keys: {list(raw_data.keys())}"
            )

            # ─────────────────────────────────────────────────────────────
            # Convert JSON into DataFrame
            # ─────────────────────────────────────────────────────────────

            if (
                "items" in raw_data
                and isinstance(raw_data["items"], list)
            ):

                logger.info(
                    "Found 'items' array. Normalizing category records."
                )

                df = pd.json_normalize(
                    raw_data["items"]
                )

            else:

                logger.warning(
                    "'items' array not found. "
                    "Normalizing entire JSON object."
                )

                df = pd.json_normalize(
                    raw_data
                )

            logger.info(
                f"Raw DataFrame shape: {df.shape}"
            )

            logger.info(
                f"DataFrame columns: {list(df.columns)}"
            )

            # ─────────────────────────────────────────────────────────────
            # Validate
            # ─────────────────────────────────────────────────────────────

            df = validate_category_data(df)

            # ─────────────────────────────────────────────────────────────
            # Add metadata
            # ─────────────────────────────────────────────────────────────

            df["_ingestion_timestamp"] = (
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

            df["_source_file"] = key

            # ─────────────────────────────────────────────────────────────
            # Extract region
            # ─────────────────────────────────────────────────────────────

            region = "unknown"

            for part in key.split("/"):

                if part.startswith("region="):

                    region = part.split(
                        "=",
                        1
                    )[1]

                    break

            df["region"] = region

            logger.info(
                f"Detected region: {region}"
            )

            logger.info(
                f"Final DataFrame shape: {df.shape}"
            )

            # ─────────────────────────────────────────────────────────────
            # Write Parquet to Silver
            # ─────────────────────────────────────────────────────────────

            logger.info(
                f"Writing Parquet to: {SILVER_PATH}"
            )

            wr.s3.to_parquet(
                df=df,
                path=SILVER_PATH,
                dataset=True,
                database=GLUE_DB,
                table=GLUE_TABLE,
                partition_cols=["region"],
                mode="overwrite_partitions",
                schema_evolution=True
            )

            logger.info(
                "Parquet write completed successfully."
            )

            processed.append(
                {
                    "key": key,
                    "region": region,
                    "rows": len(df)
                }
            )

            logger.info(
                f"Successfully processed: {key}"
            )

        except Exception as e:

            logger.error(
                f"ERROR processing {key}: {e}",
                exc_info=True
            )

            errors.append(
                {
                    "key": key,
                    "error": str(e)
                }
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────────────────────────────────

    logger.info(
        "========== Lambda execution summary =========="
    )

    logger.info(
        f"Successfully processed: {len(processed)}"
    )

    logger.info(
        f"Errors: {len(errors)}"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Alert if something failed
    # ─────────────────────────────────────────────────────────────────────────

    if errors:

        send_alert(
            subject="[YT Pipeline] Silver reference transform failed",
            message=json.dumps(
                errors,
                indent=2
            )
        )

        logger.error(
            f"Processing completed with {len(errors)} error(s)."
        )

        return {
            "statusCode": 500,
            "processed": processed,
            "errors": errors
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Successful execution
    # ─────────────────────────────────────────────────────────────────────────

    logger.info(
        "All records processed successfully."
    )

    logger.info(
        "========== Lambda execution finished =========="
    )

    return {
        "statusCode": 200,
        "processed": processed,
        "errors": []
    }