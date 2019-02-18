hdfs dfs -rm -r GENDER_OUT
python3 gender_ratio.py $MR_OPT --jobconf mapred.reduce.tasks=4 hdfs:///user/cms7202/pa_rac_S000_JT00_2015.csv --output-dir hdfs:///user/cms7202/GENDER_OUT_STATE_1 --no-output
