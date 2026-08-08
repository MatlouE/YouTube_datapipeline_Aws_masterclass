# YouTube Data Pipeline - AWS Masterclass

This repository contains a small data engineering project for downloading and organizing YouTube video statistics datasets from Kaggle and preparing them for upload to AWS S3.

## Project overview

The project includes:
- A Python script to download the Kaggle dataset into the local data directory.
- AWS-related scripts and notes for copying data into S3-compatible storage.
- Sample reference data and video statistics files under the data folder.

## Repository structure

- ingest.py: downloads the YouTube dataset into the data folder.
- data/: contains the downloaded CSV and JSON reference files.
- scripts/: contains helper shell scripts and notes for AWS operations.
- aws/: contains AWS installation resources and related documentation.

## Prerequisites

- Python 3.8+
- KaggleHub installed in your environment
- AWS CLI configured if you plan to upload data to S3

## Getting started

1. Install the required Python package:
   ```bash
   pip install kagglehub
   ```
2. Run the ingestion script:
   ```bash
   python ingest.py
   ```
3. Use the AWS scripts in the scripts directory to copy data into your S3 bucket if needed.

## Notes

- The dataset files are large and may be excluded from version control depending on your workflow.
- Ensure your AWS credentials and environment settings are configured securely.
