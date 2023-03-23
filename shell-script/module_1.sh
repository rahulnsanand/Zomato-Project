#!/bin/sh

file_status_path=/home/rahul.anand/zomato_etl/logs/module_1_status.log
mail_list="rahulanand2206@gmail.com rahul.anand@com"
module_nomen="Module1"

function mail(){
	module_name=$1
	module_status=$2
	module_starttime=$3
	module_endtime=$4
	echo -e "Subject: $module_name Status Update\n\n$module_name has completed execution!\nStatus:\t\t$module_status\nStart-Time:\t$module_starttime\nEnd-Time:\t$module_endtime\nFor more details, check zomato_etl/logs folder" | /usr/sbin/sendmail $mail_list
}


function spark_submit() {

	echo "Module 1 script has begun processing"
	json_source_path=/home/rahul.anand/zomato_etl/source/json/*
	spark_submit_value="spark-submit --driver-java-options -Dlog4j.configuration=file:/home/rahul.anand/zomato_etl/spark/scala/jsoncsv/src/main/resources/log4j-spark.properties --class Main --master yarn --deploy-mode client /home/rahul.anand/zomato_etl/spark/scala/jsoncsv/target/module1-1.0.jar"
	$spark_submit_value $json_file_name $current_date
	
	for file in $json_source_path; do
		json_file_name=$(basename "$file")
		current_date=$(date +"%Y%m%d")

		if [ $json_file_name = "file1.json" ]; then
			csv_file_name="zomato_20190609.csv"
		elif [ $json_file_name = "file2.json" ]; then
			csv_file_name="zomato_20190610.csv"
		elif [ $json_file_name = "file3.json" ]; then
			csv_file_name="zomato_20190611.csv"
		elif [ $json_file_name = "file4.json" ]; then
			csv_file_name="zomato_20190612.csv"
		elif [ $json_file_name = "file5.json" ]; then
			csv_file_name="zomato_20190613.csv"
		else
			csv_file_name="zomato_$(date +"%Y%m%d").csv"
		fi

		hdfs dfs -mv process_json/csv/$json_file_name/part-*.csv zomato_etl_rahul.anand/zomato_ext/zomato/$csv_file_name
		hdfs dfs -copyToLocal zomato_etl_rahul.anand/zomato_ext/zomato/$csv_file_name ~/zomato_etl/source/csv
		mv ~/zomato_etl/source/json/$json_file_name ~/zomato_etl/archive/
		echo "$json_file_name File has been processed $csv_file_name"
	done
	hdfs dfs -rm -r process_json
	updation "$spark_submit_value"
}

function updation(){

	spark_value=$1

	declare -a update_value
	if test -f "$file_status_path"; then

		update_value=(`cat $file_status_path`)

		beeline -u "jdbc:hive2://com.theflopguyproductions.com:1800/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n "rahul.anand" -e "insert into rahul_database.zomato_summary_log_rahul values('${update_value[0]}','${update_value[1]}','$spark_value','${update_value[2]}','${update_value[3]}','${update_
		value[4]}')"

		if [ ${update_value[4]}="SUCCESSFUL" ]; then {
			mail $module_nomen "SUCCESS" ${update_value[2]} ${update_value[3]}
		}
		elif [ ${update_value[4]}="FAILED" ]; then {
			mail $module_nomen "FAILED" ${update_value[2]} ${update_value[3]}
		}
		elif [ ${update_value[4]}="RUNNING" ]; then {
			mail $module_nomen "Unsuccessfully RUNNING" ${update_value[2]} ${update_value[3]}
		}
		else {
			mail $module_nomen "Unknown" ${update_value[2]} ${update_value[3]}
		}
		fi

		else
			echo "Unable to get updated instance"
			echo "Could not update zomato_summary_log table"
		fi
}

declare -a file_value
if test -f "$file_status_path"; then

	file_value=(`cat $file_status_path`)

	case "${file_value[4]}" in
		"SUCCESS")
			spark_submit
			;;
		"FAILED")
			echo "Previous Instance had failed"
			mail $module_nomen "Previous Instance Had Failed" ${file_value[2]} ${file_value[3]}
			spark_submit
			;;                                                
		"RUNNING")                                                
			echo "Previous instance is still running!"                                                
			echo "Aborting"                                                
			mail $module_nomen "Previous Instance is still running" ${file_value[2]} ${file_value[3]}                                                
			;;                                                
		*)                                                
			mail $module_nomen "Something went wrong, Removing status logs!" ${file_value[2]} ${file_value[3]}                                                
			echo "Something went wrong, restarting this module!"                                                
			echo "Removing corrupted status log"                                                
			rm "$file_status_path"                                                
			spark_submit                                                
			;;                                                
	esac                                                
else                                                
	echo "Status file not found, Running Spark Application"                                                
	spark_submit                                                
fi