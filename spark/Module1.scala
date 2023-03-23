import java.io.IOException
import java.io.FileNotFoundException
import java.lang.ArrayIndexOutOfBoundsException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
//import com.typesafe.scalalogging.Logger
import java.sql.Timestamp
import java.text.SimpleDateFormat;
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.io._
import org.backuity.ansi.AnsiFormatter.FormattedHelper

object Main {

//val logger = Logger("Converter Log")
	def main(args: Array[String]) = {

		var end_timestamp = "NA"
		val jsontocsv_status_location = "/home/rahul.anand/zomato_etl/logs"
		val timestamp_format = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
		val spark = SparkSession.builder().master("local").appName("Module1").getOrCreate()
		val raw_id=spark.sparkContext.applicationId
		val app_id=raw_id.toString
		val start_timestamp_retrieve = new Timestamp(System.currentTimeMillis())
		val start_timestamp = timestamp_format.format(start_timestamp_retrieve).toString

		try{
			//logger.info("Started execution of main function")
			//Initialising Spark Session
			var file_writer = new PrintWriter(new File(jsontocsv_status_location+"/module_1_status.log"))
			var job_step = "JSON-TO-CSV"
			var status = "RUNNING"
			file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
			file_writer.close()

			val conf = new Configuration()
			conf.set("fs.default.name","hdfs://com.theflopguyproductions.com")
			val hdfs = FileSystem.get(conf)

			//To Enable the usage of '$' symbol
			//By implicitly converting an RDD to a DataFrame
			import spark.implicits._

			//Prefixing Preset Values For Easier Access
			val theflopguy_host = "hdfs://com.theflopguyproductions.com/user/rahul.anand/"
			val source_dest = theflopguy_host+"zomato_etl_rahul.anand/source"
			val zomato_table_location = theflopguy_host+"zomato_etl_rahul.anand/zomato_ext/zomato"
			val json_source = theflopguy_host+"process_json/"
			val csv_dest = theflopguy_host+"zomato_etl_rahul.anand/source/csv/"
			val file_path = "/home/rahul.anand/zomato_etl/source/json/"

			//Initialising Hadoop FileSystem Structure
			if(!hdfs.exists(new Path(source_dest))){
				hdfs.mkdirs(new Path(source_dest))
			}
			if(!hdfs.exists(new Path(json_source))){
				hdfs.mkdirs(new Path(json_source))
			}
			if(!hdfs.exists(new Path(csv_dest))){
				hdfs.mkdirs(new Path(csv_dest))
			}
			if(!hdfs.exists(new Path(zomato_table_location))){
				hdfs.mkdirs(new Path(zomato_table_location))
			}

			//Getting Required Arguments For Processing and Partitioning
			val user = System.getProperty("user.name")

			println("**********************************************************************")
			println("Hello, "+user+"!")
			println("**********************************************************************")
			println("*Beginning Processing!")



			val process_file=args(0).toString //file1.json
			var file_date = args(1).toString //20190609

			if(process_file== "file1.json"){
				file_date = "20190609"
			}
			if(process_file=="file2.json"){
				file_date = "20190610"
			}
			if(process_file=="file3.json"){
				file_date = "20190611"
			}
			if(process_file=="file4.json"){
				file_date = "20190612"
			}
			if(process_file=="file5.json"){
				file_date = "20190613"
			}

			hdfs.copyFromLocalFile(new Path(file_path+process_file), new Path(json_source + process_file))
			val extension_check = process_file.split('.') // (file1, json)

			val extension=extension_check(1).toString // "json"
			val file_name = extension_check(0).toString // file1

			if(extension == "json"){

				val read_file = spark.read.json(json_source + process_file)

				println("**********************************************************************")
				println("*JSON File has been acquired successfully!")
				println("**********************************************************************")

				val explode_rdd = read_file.select(explode($"restaurants.restaurant"))

				println("**********************************************************************")
				println("*JSON File Parsing has begun successfully!")
				println("**********************************************************************")

				val final_rdd = explode_rdd.select($"col.R.res_id" as "Restaurant_Id", $"col.name" as "Restaurant Name", $"col.location.country_id" as "Country Code", $"col.location.city" as "City", $"col.location.address" as "Address", $"col.location.locality" as "Locality", $"col.location.locality_verbose" as "Locality Verbose", $"col.location.longitude" as "Longitude", $"col.location.latitude" as "Latitude", $"col.cuisines" as "Cuisines", $"col.average_cost_for_two" as "Average Cost for two", $"col.currency" as "currency", $"col.has_table_booking" as "Has table booking", $"col.has_online_delivery" as "Has Online delivery", $"col.is_delivering_now" as "Is delivering now", $"col.Switch_to_order_menu" as "Switch to order menu", $"col.price_range" as "Price range", $"col.user_rating.aggregate_rating" as "Aggregate rating",$"col.user_rating.rating_text" as "Rating text", $"col.user_rating.votes" as "Votes")

				println("**********************************************************************")
				println("*Required fields have been acquired successfully!")
				println("**********************************************************************")

				final_rdd.write.option("delimiter", "\t").csv(csv_dest+process_file) 

				println("**********************************************************************")
				println(ansi"*%green{CSV file created successfully!}")
				println("**********************************************************************")

				spark.close()

				println("**********************************************************************")
				println(ansi"*%green{Spark Session closed safely}")
				println("**********************************************************************")



				var file_writer = new PrintWriter(new File(jsontocsv_status_location+"/module_1_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "JSON-TO-CSV"
				var status = "SUCCESS"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			} else {


			}
		}
		catch{

			case e: IOException => {
				println("**********************************************************************")
				println(ansi"%red{Had an IOException trying to read that file}")
				println("**********************************************************************")
				var file_writer = new PrintWriter(new File(jsontocsv_status_location+"/module_1_status.log"))
				var end_time_retrieve = new Timestamp(System.currentTimeMillis())
				end_timestamp = timestamp_format.format(end_time_retrieve).toString
				var job_step = "JSON-TO-CSV"
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
				var job_step = "JSON-TO-CSV"
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
				var job_step = "JSON-TO-CSV"
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
				var job_step = "JSON-TO-CSV"
				var status = "FAILED"
				file_writer.write(app_id +"\t"+job_step+"\t"+start_timestamp+"\t"+end_timestamp+"\t"+status)
				file_writer.close()
			}

		}

	}
}