#!/bin/sh                                                
             
#Initialising default variables                                   
file_status_path=/home/rahul.anand/zomato_etl/logs/module_2_status.log                                                
spark_submit_value="spark-submit --driver-java-options -Dlog4j.configuration=file:/home/rahul.anand/zomato_etl/spark/scala/Module2/src/main/resources/log4j-spark.properties --class Main --master yarn --deploy-mode client /home/rahul.anand/zomato_etl/spark/scala/Module2/target/module2-1.0.jar"
mail_list="rahulanand2206@gmail.com rahul.anand@com"
module_nomen="Module2"

#Mail function to send mail on status updation
function mail(){

	module_name=$1
	module_status=$2
	module_starttime=$3
	module_endtime=$4
	module_id=$5
	echo -e "Subject: $module_name Status Update: ID-$module_id\n\n$module_name has completed execution!\nStatus:\t\t$module_status\nStart-Time:\t$module_starttime\nEnd-Time:\t$module_endtime\nFor more details, check zomato_etl/logs folder" | /usr/sbin/sendmail $mail_list

}

#Spark submit function to call the spark submit command and update the status
function spark_submit() {
	echo "Module 2 script has begun processing"
	$spark_submit_value
	updation
}

#Update function to update the status log and call the mail function
function updation(){

	declare -a update_value
	if test -f "$file_status_path"; then
		update_value=(`cat $file_status_path`)

		#Beeline command to load status log into the zomato_summary_log table
		beeline -u "jdbc:hive2://com.theflopguyproductions.com:1800/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n "rahul.anand" -e "insert into rahul_database.zomato_summary_log_rahul values('${update_value[0]}','${update_value[1]}','$spark_submit_value','${update_value[2]}','${update_value[3]}','${update_value[4]}')"

		if [ ${update_value[4]}="SUCCESSFUL" ]; then {
			mail $module_nomen "SUCCESS" ${update_value[2]} ${update_value[3]} ${update_value[0]}
		}
		elif [ ${update_value[4]}="FAILED" ]; then {
			mail $module_nomen "FAILED" ${update_value[2]} ${update_value[3]} ${update_value[0]}
		}
		elif [ ${update_value[4]}="RUNNING" ]; then {
			mail $module_nomen "Unsuccessfully exited with RUNNING" ${update_value[2]} ${update_value[3]} ${update_value[0]}
		}
		else {
			mail $module_nomen "Unknown" ${update_value[2]} ${update_value[3]} ${update_value[0]}
		}

		fi


		else
			echo "Unable to get updated instance"
			echo "Could not update zomato_summary_log table"
	fi
}

#Array to hold status.log file name
declare -a file_value

if test -f "$file_status_path"; then
	file_value=(`cat "$file_status_path"`)

	#Case to run application based on running instance check
	case "${file_value[4]}" in
		"SUCCESS")
			echo "Found SUCCESS"
			spark_submit
			;;
		"FAILED")
			echo "Previous instance failed, sent more details in mail!"
			mail $module_nomen "Previous Instance had failed!" ${file_value[2]} ${file_value[3]} ${update_value[0]}
			spark_submit
			;;
		"RUNNING")
			mail $module_nomen "Previous Instance is still running!" ${file_value[2]} ${file_value[3]} ${update_value[0]}
			echo "Previous Instance is still running"
			;;
		*)
			mail $module_nomen "Corrupted Status Logs Found, Restarting Module!" ${file_value[2]} ${file_value[3]} ${update_value[0]}
			echo "Something went wrong, restarting this module!"
			echo "Removing corrupted Status files"
			rm "$file_status_path"
			spark_submit
			;;
	esac
	else
	echo "Status file not found, Running Spark Application"
	spark_submit
fi