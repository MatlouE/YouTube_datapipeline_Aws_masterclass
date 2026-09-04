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