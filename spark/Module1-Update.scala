import java.lang.ArrayIndexOutOfBoundsException
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.{Path => HadoopPath}
import java.sql.Timestamp
import java.text.SimpleDateFormat;
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.io._
import org.backuity.ansi.AnsiFormatter.FormattedHelper
import java.io.File
import java.nio.file.{Files => NioFiles, Paths => NioPaths, StandardCopyOption}

object Main {

	//Function to return list of files and extension in given directory
	def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
	    dir.listFiles.filter(_.isFile).toList.filter { file =>
	        extensions.exists(file.getName.endsWith(_))
	    }
	}

	//Main Function begins here
	def main(args: Array[String]) = {

		//Initialising timestamp variable to keep track of this running instance 
		var end_timestamp = "NA"
		val start_timestamp_retrieve = new Timestamp(System.currentTimeMillis())
		val start_timestamp = timestamp_format.format(start_timestamp_retrieve).toString
		val job_step = "JSON-TO-CSV"
		val jsontocsv_status_location = "/home/rahul.anand/zomato_etl/logs"
		val timestamp_format = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
		//Building Spark Session
		val spark = SparkSession.builder().master("local").appName("Module1").getOrCreate()
		//Retrieving Spark Application ID
		val raw_id=spark.sparkContext.applicationId
		val app_id=raw_id.toString
		
		//Initiating Try Method to catch any run time errors
		try{
			//Opening a file module_1_status.log to write the current status of the application
			var file_writer = new PrintWriter(new File(jsontocsv_status_location+"/module_1_status.log"))
			var status = "RUNNING"
			file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
			file_writer.close()

			//Creating an HDFS configuration for access hdfs directories and performing actions
			val conf = new Configuration()
			conf.set("fs.default.name","hdfs://com.theflopguyproductions.com")
			val hdfs = FileSystem.get(conf)

			//To Enable the usage of '$' symbol
			//By implicitly converting an RDD to a DataFrame
			import spark.implicits._

			//Prefixing Preset Location Values For Easier Access
			val theflopguy_host = "hdfs://com.theflopguyproductions.com/user/rahul.anand/"
			val rahul_home_hdfs = theflopguy_host+"zomato_etl_rahul.anand"
			val json_source = theflopguy_host+"process_json/"
			val csv_dest = theflopguy_host+"process_json/csv/"
			val file_path = "/home/rahul.anand/zomato_etl/source/json/"
			val json_extension_check = List("json")
			val file_path_loop = getListOfFiles(new File("/home/rahul.anand/zomato_etl/source/json"), json_extension_check)

			//Initialising Hadoop FileSystem Structure
			if(!hdfs.exists(new Path(json_source))){hdfs.mkdirs(new Path(json_source))}
			if(!hdfs.exists(new Path(csv_dest))){hdfs.mkdirs(new Path(csv_dest))}
			if(!hdfs.exists(new Path(rahul_home_hdfs))){hdfs.mkdirs(new Path(rahul_home_hdfs))}
			if(!hdfs.exists(new Path(rahul_home_hdfs+"/zomato_ext"))){hdfs.mkdirs(new Path(rahul_home_hdfs+"/zomato_ext"))}
			if(!hdfs.exists(new Path(rahul_home_hdfs+"/zomato_ext/zomato"))){hdfs.mkdirs(new Path(rahul_home_hdfs+"/zomato_ext/zomato"))}

			//Getting current username
			val user = System.getProperty("user.name")
			

			println("**********************************************************************")
			println("	Hello, "+user+"!")
			println("**********************************************************************")
			println("*	Beginning Processing!")

			//Running a for loop to loop through files in the local filesystem (Eg. file1.json, file2.json...)
			for (raw_file<-file_path_loop) {

				//Splitting file path to retrieve the file name and extension
				val process_file_string=raw_file.toString
				val process_slash_file=process_file_string.split('/') //home/rahul.anand/zomato_etl/source/json/file1.json
				val process_file=process_slash_file(6)
				var file_date = args(0).toString //20190609

				//Getting Required Arguments For Processing and Partitioning
				if(process_file== "file1.json"){ file_date = "20190609"}
				if(process_file=="file2.json"){	file_date = "20190610"}
				if(process_file=="file3.json"){	file_date = "20190611"}
				if(process_file=="file4.json"){	file_date = "20190612"}
				if(process_file=="file5.json"){	file_date = "20190613"}


				//Copying required file from local folder to hdfs temporary directory for processing
				hdfs.copyFromLocalFile(new Path(file_path+process_file), new Path(json_source + process_file))

				//Creating an RDD to read the json file 
				val read_file = spark.read.json(json_source + process_file)

				println("**********************************************************************")
				println("*	JSON File has been acquired successfully!")
				println("**********************************************************************")

				//Explode on the json unstructured data to retrieve required columns
				val explode_rdd = read_file.select(explode($"restaurants.restaurant"))

				println("**********************************************************************")
				println("*	JSON File Parsing has begun successfully!")
				println("**********************************************************************")

				//Retrieving required columns from json
				val final_rdd = explode_rdd.select($"col.R.res_id" as "Restaurant_Id", $"col.name" as "Restaurant Name", $"col.location.country_id" as "Country Code", $"col.location.city" as "City", $"col.location.address" as "Address", $"col.location.locality" as "Locality", $"col.location.locality_verbose" as "Locality Verbose", $"col.location.longitude" as "Longitude", $"col.location.latitude" as "Latitude", $"col.cuisines" as "Cuisines", $"col.average_cost_for_two" as "Average Cost for two", $"col.currency" as "currency", $"col.has_table_booking" as "Has table booking", $"col.has_online_delivery" as "Has Online delivery", $"col.is_delivering_now" as "Is delivering now", $"col.Switch_to_order_menu" as "Switch to order menu", $"col.price_range" as "Price range", $"col.user_rating.aggregate_rating" as "Aggregate rating",$"col.user_rating.rating_text" as "Rating text", $"col.user_rating.votes" as "Votes")

				println("**********************************************************************")
				println("*	Required fields have been acquired successfully!")
				println("**********************************************************************")

				//Writing the required columns into a temporary destination csv path
				final_rdd.write.option("delimiter", "\t").csv(csv_dest+process_file) 

				println("**********************************************************************")
				println(ansi"*	%green{CSV file created successfully :}")
				print(process_file+" => "+"zomato_"+file_date+".csv")
				println("**********************************************************************")

				//Initialising Path variables to rename, move and copy to local execution of the csv file as well as deleting the temporary directory
				val part_file = hdfs.globStatus(new Path("hdfs://com.theflopguyproductions.com/user/rahul.anand/process_json/csv/"+process_file+"/part*"))(0).getPath().getName()
				val csv_zomato_destination = new Path("hdfs://com.theflopguyproductions.com/user/rahul.anand/zomato_etl_rahul.anand/zomato_ext/zomato/zomato_"+file_date+".csv")
				val part_file_source = new Path("hdfs://com.theflopguyproductions.com/user/rahul.anand/process_json/csv/"+process_file+"/"+part_file)
				val csv_local=new HadoopPath("/home/rahul.anand/zomato_etl/source/csv/zomato_"+file_date+".csv")
				val json_local=NioPaths.get("/home/rahul.anand/zomato_etl/source/json/"+process_file)
				val archive_local=NioPaths.get("/home/rahul.anand/zomato_etl/archive/"+process_file)

				//renaming and moving the csv file to the zomato table location
				hdfs.rename(part_file_source, csv_zomato_destination)
				//copying the csv file to the local filesystem location
				hdfs.copyToLocalFile(csv_zomato_destination, csv_local)
				//moving the processed json files from source/json to archive folder in the local file system
				NioFiles.move(json_local, archive_local, StandardCopyOption.REPLACE_EXISTING)
				
			}
			//Deleting the temporary hdfs directory
			val temp_hdfs_dir = new Path(json_source)
			hdfs.delete(temp_hdfs_dir, true)
			spark.close()

			println("**********************************************************************")
			println(ansi"*%green{Spark Session closed safely}")
			println("**********************************************************************")

			//Writing the status log file to "SUCCESS" status after closing the spark session
			file_writer = new PrintWriter(new File(jsontocsv_status_location+"/module_1_status.log"))
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
				var file_writer = new PrintWriter(new File(jsontocsv_status_location+"/module_1_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
			case e: FileNotFoundException => {
				println("**********************************************************************")
				println(ansi"%red{Caught File Not Found Exception!}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(jsontocsv_status_location+"/jsontocsv_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
			case e: ArrayIndexOutOfBoundsException => {
				println("**********************************************************************")
				println(ansi"%red{Caught Array Index Out Of Bounds Exception, Kindly Input Parameters On Invoking This Script}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(jsontocsv_status_location+"/jsontocsv_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
			case _: Throwable  => {
				println("**********************************************************************")
				println(ansi"%red{Caught an Error, Kindly Refer Logs. Failed Status Updated!}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(jsontocsv_status_location+"/module_1_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}
		}
	}
}