hdfs dfs -rm -r [INSERT_OUTPUT_DIR_NAME]
python3 [INSERT_SCRIPT_NAME] $MR_OPT --jobconf mapred.reduce.tasks=4 hdfs:///user/[INSERT_USERNAME]/pa_rac_S000_JT00_2015.csv --output-dir hdfs:///user/[INSERT_USER_NAME]/[INSERT_OUTPUT_DIR_NAME] --no-output
