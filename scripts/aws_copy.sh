# Upload CSV trending statistics (partitioned by region)
aws s3 cp CAvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=ca/CAvideos.csv
aws s3 cp DEvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=de/DEvideos.csv
aws s3 cp FRvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=fr/FRvideos.csv
aws s3 cp GBvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=gb/GBvideos.csv
aws s3 cp INvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=in/INvideos.csv
aws s3 cp JPvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=jp/JPvideos.csv
aws s3 cp KRvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=kr/KRvideos.csv
aws s3 cp MXvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=mx/MXvideos.csv
aws s3 cp RUvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=ru/RUvideos.csv
aws s3 cp USvideos.csv s3://yt-data-pipeline-bronze-e/youtube/raw_statistics/region=us/USvideos.csv

# Upload JSON reference category data (partitioned by region)
aws s3 cp CA_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=ca/CA_category_id.json
aws s3 cp DE_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=de/DE_category_id.json
aws s3 cp FR_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=fr/FR_category_id.json
aws s3 cp GB_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=gb/GB_category_id.json
aws s3 cp IN_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=in/IN_category_id.json
aws s3 cp JP_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=jp/JP_category_id.json
aws s3 cp KR_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=kr/KR_category_id.json
aws s3 cp MX_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=mx/MX_category_id.json
aws s3 cp RU_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=ru/RU_category_id.json
aws s3 cp US_category_id.json s3://yt-data-pipeline-bronze-e/youtube/raw_statistics_reference_data/region=us/US_category_id.json