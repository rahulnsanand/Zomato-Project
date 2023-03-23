#!/bin/sh

#Initialising variable with default data
log_path=/home/rahul.anand/zomato_etl/logs/
current_date=$(date +"%d%m%Y")

echo "Purging Begins"

#Looping through the list of all files in the path
for file in `ls $log_path`;
do
	#Retrieving only the file name from the list
	file_name="$(basename "$file")"

	#Searching of the specific naming pattern log
	file_name_pattern=".{7}_log_.{8}_.*.log"
	if [[ $file_name =~ $file_name_pattern ]]; then

		#Retreiving the creation date from the name of the log file
		log_date=$(awk -F '_' '{print $3}' <<< $file_name)
		#Calculating difference
		difference="$(($current_date - $log_date))"
		#Checking the constraint
		if (($difference>0 && $difference<70000000)); then
			echo "Deleting Log :" $file_name
			#Deleting the log file
			rm "$log_path$file_name"
		fi
	fi

done

echo "Purge Complete!"