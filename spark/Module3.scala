import org.apache.spark.sql.SparkSession
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.io.IOException
import java.io._
import java.io.FileNotFoundException
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.backuity.ansi.AnsiFormatter.FormattedHelper

object Main{

	//Main Function begins here
	def main(args: Array[String]) = {

		//Initialising timestamp variable to keep track of this running instance
		val loadsummary_status_location = "/home/rahul.anand/zomato_etl/logs"
		var end_timestamp = "NA"
		val timestamp_format = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
		val start_timestamp_retrieve = new Timestamp(System.currentTimeMillis())
		//Building Spark Session
		val start_timestamp = timestamp_format.format(start_timestamp_retrieve).toString
		val spark = SparkSession.builder().master("local").appName("Module3").getOrCreate()
		//Retrieving Spark Application ID
		val raw_id=spark.sparkContext.applicationId
		val app_id=raw_id.toString
		
		//Initiating Try Method to catch any run time errors
		try{
			//Opening a file module_1_status.log to write the current status of the application
			var file_writer = new PrintWriter(new File(loadsummary_status_location+"/module_3_status.log"))
			var job_step = "LOAD-ZOMATO-SUMMARY"

			var status = "RUNNING"

			file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)

			file_writer.close()

			val input_con = System.console()
			val user = System.getProperty("user.name")

			println("**********************************************************************")
			println("*Hello, "+user+"!")
			println("**********************************************************************")
			println("*Proceeding with loading data!")
			println("**********************************************************************")


			//obtaining HiveDriver package to set Class for JDBC connection
			val driver_name = "org.apache.hive.jdbc.HiveDriver";


			//Initialise Class with driver_name
			Class.forName(driver_name);

			println("**********************************************************************")
			println("*Establishing JDBC Connnection")
			println("**********************************************************************")


			//Establishing JDBC Connection
			val jdbc_connect = DriverManager.getConnection("jdbc:hive2://com.theflopguyproductions.com:1800/default;password=rahul.anand;serviceDiscoveryMode=zooKeeper;user=rahul.anand;zooKeeperNamespace=hiveserver2", "rahul.anand", "rahul.anand123")


			//Initialising a jdbc statement to enable execution of queries, etc
			val hive_statement = jdbc_connect.createStatement()

			//Initialising a query_text var to enable input of query in String format
			var query_text = " " 

			//Initialising a choice variable
			var choice = 0

			println("**********************************************************************")
			println(ansi"*%green{Connection Established Successfully}")
			println("**********************************************************************")

			query_text = "USE rahul_database"
			hive_statement.execute(query_text)

			println("**********************************************************************")
			println(ansi"*%green{Connected to Database rahul_database}")
			println("**********************************************************************")

			//Get Input Option From User
			if(args.length>0){
				if(args(0)=="1"){
					println("**********************************************************************")
					println("*")
					println("*Proceeding with updation with criteria")
					println("*")
					choice = 1
				}
				if(args(0)=="2"){

					println("**********************************************************************")
					println("*")
					println("*      Proceeding with historical updation")
					println("*")

					choice = 2
				}
			}

			if(args.length==0){
				println("**********************************************************************")
				println("*")
				println("*Enter an Apt Option To Proceed")
				println("*")
				println(ansi"%yellow{To Load Data With FileData and Country Specified-->1}")
				println(ansi"%yellow{To Load Data Historically-->2}")
				print  (">>")

				choice = Integer.parseInt(input_con.readLine())
			}

			println("**********************************************************************")

			//Create zomato_summary table if it doesn't exist
			query_text = "CREATE TABLE IF NOT EXISTS zomato_summary_rahul(`Restaurant ID` INT,`Restaurant Name` STRING,`Country Code` INT,`City` STRING, `Address` STRING,`Locality` STRING,`Locality Verbose` STRING,`Longitude` STRING,`Latitude` STRING,`Cuisines` STRING,`Average Cost for two` INT,`Currency` STRING,`Has Table booking` INT,`Has Online delivery` INT,`Is delivering now` INT,`Switch to order menu` INT,`Price range` INT,`Aggregate rating` STRING,`Rating text` STRING,`Votes` STRING, `m_rating_colour` STRING, `m_cuisines` STRING, `create_datetime` TIMESTAMP, `user_id` STRING)  PARTITIONED BY ( p_filedate INT, p_country_name STRING ) stored as ORC"
			hive_statement.execute(query_text)

			println("**********************************************************************")
			println("*Managed zomato_summary table is all set to receive data")
			println("**********************************************************************")

			choice match {
				case 1 => {

					//Manual data updation
					println("**********************************************************************")
					print("*Please Enter Filedate to load >>")
					val file_date = input_con.readLine()
					println("**********************************************************************")
					print("*Please Enter Country Name to load >>")
					val country_name = input_con.readLine()
					println("**********************************************************************")

					query_text = "INSERT INTO zomato_summary_rahul(`restaurant id`, `restaurant name`, `country code`,`city`,`address`,`locality`,`locality verbose`,`longitude`,`latitude`,`cuisines`,`average cost for two`,`currency`,`has table booking`,`has online delivery`,`is delivering now`,`switch to order menu`,`price range`,`aggregate rating`,`rating text`,`votes`,`p_filedate`, `p_country_name`, `m_cuisines`, `m_rating_colour`, `create_datetime`, `user_id`) SELECT  s.`restaurant id`, nvl( s.`restaurant name`,'NA'), s.`country code`, nvl(s.`city`, 'NA'), nvl(s.`address`, 'NA'), nvl( s.`locality`,'NA'), nvl(s.`locality verbose`,'NA'), nvl( s.`longitude`, 'NA'), nvl( s.`latitude`, 'NA'), nvl(s.`cuisines`,'NA'),s.`average cost for two`, nvl(s.`currency`,'NA'), s.`has table booking`, s.`has online delivery`,s.`is delivering now`,s.`switch to order menu`,s.`price range`,nvl(s.`aggregate rating`,'NA'), nvl(s.`rating text`,'NA'), nvl(s.`votes`,'NA'),s.`filedate`, nvl(d.`country`,'NA') ,  case when cuisines like any ('%Indian%', '%Andhra%', '%Hyderabadi%', '%Goan%', '%Bengali%', '%Bihari%', '%Chettinad%', '%Gujarati%','%Rajasthani%', '%Kerala%', '%Maharashtrian%','%Mangalorean%', '%Mithai%','%Mughlai%') then 'Indian' else 'World Cuisines' end as m_cuisines, case when `aggregate rating`  between 1.9 and 2.4 and `rating text` = 'Poor' then 'Red' when `aggregate rating`  between 2.5 and 3.4 and `rating text`='Average' then 'Amber' when `aggregate rating` between 3.5 and 3.9 and `rating text`='Good' then 'Light Green' when `aggregate rating` between 4.0 and 4.4 and `rating text`='Very Good' then 'Green' when `aggregate rating` between 4.5 and 5 and `rating text`='Excellent' then 'Gold' when `aggregate rating` = 0 and `rating text`='Not rated' then 'NA'  end as m_rating_colour, from_unixtime(unix_timestamp()), logged_in_user() from zomato_rahul s, dim_country_rahul d where s.`country code` = d.`country code` and s.filedate = '"+file_date+"' and d.country = '"+country_name+"'"

					hive_statement.execute(query_text)

					println("**********************************************************************")
					println("*Loaded Data into zomato_summary table With Specified Criteria")
					println("**********************************************************************")

					query_text = "INSERT OVERWRITE TABLE zomato_summary_rahul SELECT DISTINCT * FROM zomato_summary_rahul"
					hive_statement.execute(query_text)

					println("**********************************************************************")
					println("*Duplicate values removed from table zomato_rahul")
					println("**********************************************************************")


				}

				case 2 => {

					//Historical data updation
					query_text = "INSERT INTO zomato_summary_rahul(`restaurant id`, `restaurant name`, `country code`,`city`,`address`,`locality`,`locality verbose`,`longitude`,`latitude`,`cuisines`,`average cost for two`,`currency`,`has table booking`,`has online delivery`,`is delivering now`,`switch to order menu`,`price range`,`aggregate rating`,`rating text`,`votes`,`p_filedate`, `p_country_name`, `m_cuisines`, `m_rating_colour`, `create_datetime`, `user_id`) SELECT  s.`restaurant id`, nvl( s.`restaurant name`,'NA'), s.`country code`, nvl(s.`city`, 'NA'), nvl(s.`address`, 'NA'), nvl( s.`locality`,'NA'), nvl(s.`locality verbose`,'NA'), nvl( s.`longitude`, 'NA'), nvl( s.`latitude`, 'NA'), nvl(s.`cuisines`,'NA'),s.`average cost for two`, nvl(s.`currency`,'NA'), s.`has table booking`, s.`has online delivery`,s.`is delivering now`,s.`switch to order menu`,s.`price range`,nvl(s.`aggregate rating`,'NA'), nvl(s.`rating text`,'NA'), nvl(s.`votes`,'NA'),s.`filedate`, nvl(d.`country`,'NA') ,  case when cuisines like any ('%Indian%', '%Andhra%', '%Hyderabadi%', '%Goan%', '%Bengali%', '%Bihari%', '%Chettinad%', '%Gujarati%','%Rajasthani%', '%Kerala%', '%Maharashtrian%','%Mangalorean%', '%Mithai%','%Mughlai%') then 'Indian' else 'World Cuisines' end as m_cuisines, case when `aggregate rating`  between 1.9 and 2.4 and `rating text` = 'Poor' then 'Red' when `aggregate rating`  between 2.5 and 3.4 and `rating text`='Average' then 'Amber' when `aggregate rating` between 3.5 and 3.9and `rating text`='Good' then 'Light Green' when `aggregate rating` between 4.0 and 4.4 and `rating text`='Very Good' then 'Green' when `aggregate rating` between 4.5 and 5 and `rating text`='Excellent' then 'Gold' when `aggregate rating` = 0 and `rating text`='Not rated' then 'NA'  end as m_rating_colour, from_unixtime(unix_timestamp()), logged_in_user() from zomato_rahul s, dim_country_rahul d where s.`country code` = d.`country code` "

					hive_statement.execute(query_text)

					println("**********************************************************************")
					println("*Historically Loaded all Data into zomato_summary!")
					println("**********************************************************************")

					query_text = "INSERT OVERWRITE TABLE zomato_summary_rahul SELECT DISTINCT * FROM zomato_summary_rahul"
					hive_statement.execute(query_text)
					   
					println("**********************************************************************")
					println("*      Duplicate values removed from table zomato_rahul")
					println("**********************************************************************")

				}

				case _ => {

					//Default case called if wrong input given
					println("**********************************************************************")
					println(ansi"*%red{Invalid Option Chosen!}")
					println("**********************************************************************")
				}

				println("**********************************************************************")
				println(ansi"*%green{Module 3 Completed!}")
				println("**********************************************************************")

			}

			//Closing All Connections Safely
			hive_statement.close()
			jdbc_connect.close()
			spark.close()
			
			//Writing the status log file to "SUCCESS" status after closing the spark session
			file_writer = new PrintWriter(new File(loadsummary_status_location+"/module_3_status.log"))
			var end_time_retrieve = new Timestamp(System.currentTimeMillis())
			end_timestamp = timestamp_format.format(end_time_retrieve).toString
			job_step = "LOAD-ZOMATO-SUMMARY-TABLE"
			status = "SUCCESS"
			file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
			file_writer.close()

		}
		catch{

			case e: IOException => {
				println("**********************************************************************")
				println(ansi"%red{Had an IOException trying to read that file}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(loadsummary_status_location+"/module_3_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "LOAD-ZOMATO-SUMMARY-TABLE"
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
			case e: FileNotFoundException => {
				println("**********************************************************************")
				println(ansi"%red{Caught File Not Found Exception!}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(loadsummary_status_location+"/module_3_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "LOAD-ZOMATO-SUMMARY-TABLE"
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
			case _: Throwable => {
				println("**********************************************************************")
				println(ansi"%red{Got some other kind of exception, Exiting!}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(loadsummary_status_location+"/module_3_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "LOAD-ZOMATO-SUMMARY-TABLE"
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
			case e: ArrayIndexOutOfBoundsException => {
				println("**********************************************************************")
				println(ansi"%red{Caught Array Index Out Of Bounds Exception, Kindly Input Parameters On Invoking This Script}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(loadsummary_status_location+"/module_3_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "LOAD-ZOMATO-SUMMARY-TABLE"
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
		}
	}
}
