S3
Bronze bucket Name - yt-data-pipeline-bronze-e 
Silver Bucket Name - yt-data-pipeline-silver-e 
Gold Bucket Name - yt-data-pipeline-gold-e 
script bucket name - yt-data-pipeline-script-e 

Glue Databases
Glue Bronze - yt-pipeline-bronze-dev
Glue silver - yt-pipeline-silver-dev
Glue Gold - yt_pipeline_gold_dev

SNS
sns-arn:aws:sns:af-south-1:726621696217:yt-data-pipeline-alerts-dev

Glue job bronze_to_silver parameters
--bronze_database yt-pipeline-bronze-dev
--bronze_table raw_statistics
--silver_bucket yt-data-pipeline-silver-e
--silver_database yt-pipeline-silver-dev
--silver_table clean_statistics

Glue job silver_to_gold parameters
--silver_database yt-pipeline-silver-dev
--gold_bucket yt-data-pipeline-gold-e 
--gold_database yt_pipeline_gold_dev

