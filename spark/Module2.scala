import org.apache.spark.sql.SparkSession
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.FileNotFoundException
import java.lang.ArrayIndexOutOfBoundsException
import java.sql.DriverManager;
import java.io.IOException
import org.apache.hadoop.fs._
import java.io._
import org.backuity.ansi.AnsiFormatter.FormattedHelper
import java.sql.Timestamp
import java.text.SimpleDateFormat

object Main{

	//Main Function begins here
	def main(args: Array[String]) = {

		//Initialising timestamp variable to keep track of this running instance 
		val timestamp_format = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
		val start_timestamp_retrieve = new Timestamp(System.currentTimeMillis())
		val start_timestamp = timestamp_format.format(start_timestamp_retrieve).toString
		val loadzomato_status_location = "/home/rahul.anand/zomato_etl/logs"
		//Building Spark Session
		val spark = SparkSession.builder().master("local").appName("Module2").getOrCreate()
		//Retrieving Spark Application ID
		val raw_id=spark.sparkContext.applicationId
		val app_id=raw_id.toString
		var end_timestamp = "NA"
		var job_step = "LOAD-CSV-TO-ZOMATO-TABLE"
		//Initiating Try Method to catch any run time errors
		try{
			
			var status = "RUNNING"
			//Opening a file module_1_status.log to write the current status of the application
			var file_writer = new PrintWriter(new File(loadzomato_status_location+"/module_2_status.log"))
			file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
			file_writer.close()

			//Creating an HDFS configuration for access hdfs directories and performing actions
			val hdfs_configuration = spark.sparkContext.hadoopConfiguration

			//Prefixing Preset Location Values For Easier Access
			val load_file_path = new Path("hdfs://com.theflopguyproductions.com/user/rahul.anand/zomato_etl_rahul.anand/zomato_ext/zomato")
			val file_system = load_file_path.getFileSystem(hdfs_configuration)
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

			println("**********************************************************************")
			println(ansi"*%green{Connection Established Successfully}")
			println("**********************************************************************")

			query_text = "USE rahul_database"
			hive_statement.execute(query_text)

			println("**********************************************************************")
			println(ansi"*%green{Connected to Database rahul_database}")
			println("**********************************************************************")

			//Creating zomato table if not exists
			query_text = "CREATE EXTERNAL TABLE IF NOT EXISTS rahul_database.zomato_rahul(`Restaurant ID` INT,`Restaurant Name` STRING,`Country Code` INT,`City` STRING, `Address` STRING,`Locality` STRING,`Locality Verbose` STRING,`Longitude` STRING,`Latitude` STRING,`Cuisines` STRING,`Average Cost for two` INT,`Currency` STRING,`Has Table booking` INT,`Has Online delivery` INT,`Is delivering now` INT,`Switch to order menu` INT,`Price range` INT,`Aggregate rating` STRING,`Rating text` STRING,`Votes` STRING) PARTITIONED BY ( filedate int ) ROW FORMAT DELIMITED fields terminated by '\t'stored as textfile"

			hive_statement.execute(query_text)

			println("**********************************************************************")
			println("*External zomato table is all set to receive data")
			println("**********************************************************************")

			//Setting location of zomato table
			query_text = "ALTER TABLE zomato_rahul SET LOCATION 'hdfs://theflopguyproductions/user/rahul.anand/zomato_etl_rahul.anand/zomato_ext/zomato'"
			hive_statement.execute(query_text)

			println("**********************************************************************")
			println("*Set Location for zomato_rahul Table!")
			println("**********************************************************************")

			//Run a for loop to loop through the csv files located in the given location
			val file_status = file_system.globStatus(new Path(load_file_path+"/zomato_*"))
			for (urlStatus <- file_status) {

				//Split the filename to get extension to avoid IOException
				var file_name=urlStatus.getPath.getName
				var dot_split = file_name.split('.')// zomato_20190609, csv || 20190609
				var first_name = dot_split(0).toString//zomato_20190609		|| 
				var extension = dot_split(1).toString//csv
				var underscore_split = first_name.split('_')
				var partition_name = underscore_split(1).toString
				var file_path = load_file_path+"/"+file_name
				println(file_name)

				//Loading Data Into zomato Table
				query_text = "LOAD DATA INPATH '"+file_path+"' OVERWRITE INTO TABLE zomato_rahul PARTITION (filedate = '"+partition_name+"')"
				hive_statement.execute(query_text)

				println(ansi"%green{**********************************************************************}")
				println("*File "+file_name+" Loaded and Partition "+partition_name+" Created!")
				println(ansi"%green{**********************************************************************}")

			}

			//Closing All Connections Safely
			hive_statement.close()
			jdbc_connect.close()
			spark.close()

			//Writing the final status of the execution as "SUCCESS"
			file_writer = new PrintWriter(new File(loadzomato_status_location+"/module_2_status.log"))
			var end_time_retrieve = new Timestamp(System.currentTimeMillis())
			end_timestamp = timestamp_format.format(end_time_retrieve).toString
			status = "SUCCESS"
			file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
			file_writer.close()

		}
		catch{
			case e: IOException => {
				println("**********************************************************************")
				println(ansi"%red{Had an IOException trying to read that file}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(loadzomato_status_location+"/module_2_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "LOAD-TO-ZOMATO-TABLE"
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()

			}

			case e: FileNotFoundException => {

				println("**********************************************************************")
				println(ansi"%red{Caught File Not Found Exception!}")
				println("**********************************************************************")

				var file_writer = new PrintWriter(new File(loadzomato_status_location+"/module_2_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "LOAD-TO-ZOMATO-TABLE"
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()

			}

			case e: ArrayIndexOutOfBoundsException => {

				println("**********************************************************************")
				println(ansi"%red{Caught Array Index Out Of Bounds Exception, Kindly Input Parameters On Invoking This Script}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(loadzomato_status_location+"/module_2_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "LOAD-TO-ZOMATO-TABLE"
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
			case _: Throwable  => {
				println("**********************************************************************")
				println(ansi"%red{Caught an Error, Kindly Refer Logs. Failed Status Updated!}")
				println("**********************************************************************")
				 var file_writer = new PrintWriter(new File(loadzomato_status_location+"/module_2_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "LOAD-TO-ZOMATO-TABLE"
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
		}
	}
}