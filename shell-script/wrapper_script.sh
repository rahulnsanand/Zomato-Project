#!/bin/sh

#Gets option from User
opt=$1

#Default usage function to help the user incase of a wrong argument
function usage() {
	echo "usage:${0} OPTION"
	echo "1 <- Execute Module 1 [Load CSV File]"
	echo "2 <- Execute Module 2 [Load into zomato table]"
	echo "3 <- Execute Module 3 [Load into zomato_summary table]"
	echo "4 <- Execute Module 4 [Run all the modules in preset sequence]" 
	exit 1
}


#Runs a case statement on the argument given by user
case $opt in

	#Module 1 is called along with purge_logs
	1)
		bash /home/rahul.anand/zomato_etl/script/module_1.sh
		bash /home/rahul.anand/zomato_etl/script/purge_logs.sh
		;;

	#Module 2 is called along with purge_logs
	2)
		bash /home/rahul.anand/zomato_etl/script/module_2.sh
		bash /home/rahul.anand/zomato_etl/script/purge_logs.sh
		;;

	#Module 3 is called along with purge_logs
	3)
		bash /home/rahul.anand/zomato_etl/script/module_3.sh
		bash /home/rahul.anand/zomato_etl/script/purge_logs.sh
		;;

	#All the modules are called along with purge_logs
	4)
		echo "Automated Execution Begins"
		bash /home/rahul.anand/zomato_etl/script/module_1.sh
		bash /home/rahul.anand/zomato_etl/script/module_2.sh
		bash /home/rahul.anand/zomato_etl/script/module_3.sh "2"
		bash /home/rahul.anand/zomato_etl/script/purge_logs.sh
		;;

	#Default Module is called with usage function
	*)
		usage
		exit 1
		;;
esac