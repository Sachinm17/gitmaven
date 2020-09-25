package com.project.FraudDetection
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
object ProcessingHDFSLogs {
  
   //Create a class representing the schema of dataset.
  case class TransactionLogs (CustomerID:String, CreditCardNo:Long, TransactionLocation:String, TransactionAmount:Int, TransactionCurrency:String, MerchantName:String, NumberofPasswordTries:Int, TotalCreditLimit:Int, CreditCardCurrency:String )
  
  def main(args: Array[String]) {
    
    //validating number of command line arguments.
    if (args.length < 1) {
      System.err.println("Please provide HDFS folder path")
      System.exit(1)
    }  
    
    // Create Streaming context with 10 second batch interval
    val sparkConf = new SparkConf().setAppName("SparkStreamingKafkaFraudAnalysis")
   
    val ssc = new StreamingContext(sparkConf, Seconds(10))  
   
   //consume the messages from HDFS folder and create DStream
    //args(0) - HDFS folder path
    val LogsStream = ssc.textFileStream(args(0))
   
        
    //Collecting the records for window period of 60seconds and sliding interval time 20 seconds
    val windowedLogsStream = LogsStream.window(Seconds(60),Seconds(20));
    
    //create SQL context object to perform SQL operations
    val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
    import sqlContext.implicits._    
    
   //Process each RDD of stream and detect fraud.  
   windowedLogsStream.foreachRDD(
       
   rdd =>
   {
   //converting RDD in to Dataframe
   val df = rdd.map(x => x.split(",")).map { c  => TransactionLogs(c(0), c(1).trim.toLong, c(2), c(3).trim.toInt, c(4), c(5), c(6).trim.toInt, c(7).trim.toInt, c(8)) }.toDF()
    
   
   df.registerTempTable("CustomerTransactionLogs")
    
    //writing SQL query on Dataframe to detect fraud.
  val fraudDF = sqlContext.sql("Select CustomerID, CreditCardNo, TransactionAmount, TransactionCurrency, NumberofPasswordTries, TotalCreditLimit, CreditCardCurrency from CustomerTransactionLogs where NumberofPasswordTries > 3 OR TransactionCurrency != CreditCardCurrency OR ( TransactionAmount * 100.0 / TotalCreditLimit ) > 50");
    
    //display customer details to whom fraud alert has to be sent
    fraudDF.show();    
     
     }
   )
   
   //Start the Streaming application
   ssc.start()
    
    ssc.awaitTermination()       
  }      
}
