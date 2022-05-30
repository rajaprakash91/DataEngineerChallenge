package raja.paypay.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object LogFileAnalytics extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // if user not passes exact two arguements
    if (args.length < 2 || args.length > 3) {
      println(s"Pls. provide input text file location and output folder location to save ")
      System.exit(-1)
    }
    // Example "/Users/r0m0831/other_git/DataEngineerChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz" "/Users/r0m0831/other_git/DataEngineerChallenge/data"

    // Create Spark Session
    @transient lazy val spark = SparkSession.builder()
      .appName(" PayPayDEChallenge")
      .master("local[3]")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()

    // Read from Zip File Directly and create Raw DF
    //    val rawFilePath = "/Users/r0m0831/other_git/paypayde/data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
    //    val outPutFilePath = "/Users/r0m0831/other_git/paypayde/notebook"

    val rawFilePath = args(0)
    val outPutFilePath = args(1)

    println(s"Input File location: $rawFilePath")
    println(s"OutPut File location: $outPutFilePath")


    val rawDF = spark.read.textFile(rawFilePath).toDF()

    // Get RegEx Parsed Dataframe

    val parsedDF = utils.rawParserFunction(rawDF)
    parsedDF.printSchema()


    // Get Analytical Ready DF by adding calculative columns
    val maxActiveSsn = 900 // Set Max Active session time in seconds.
    val logDF = utils.addingAdditionalCols(parsedDF, maxActiveSsn)

    logDF.printSchema()

    // Assignements Questions and Processed Dataframe

    //    1.Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    val dfSessionize = utils.fnSessionize(logDF)

    dfSessionize.show()


    //    2. Determine the average session time
    val dfAvg = utils.fnAverageSessionTime(logDF)

    dfAvg.show()

    //    3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    val dfUnique = utils.fnUniqueUrlHits(logDF)

    dfUnique.show()

    //    4.Find the most engaged users, ie the IPs with the longest session times
    val dfLongestSession = utils.fnAverageSessionTime(logDF)

    dfLongestSession.show()

    // Write Output file into CSV

    dfSessionize.coalesce(1).write.option("header", true).csv(s"$outPutFilePath/log_sessionised.csv")
    dfAvg.coalesce(1).write.option("header", true).csv(s"$outPutFilePath/average_session_time.csv")
    dfUnique.coalesce(1).write.option("header", true).csv(s"$outPutFilePath/unique_url_hits.csv")
    dfLongestSession.coalesce(1).write.option("header", true).csv(s"$outPutFilePath/ip_longest_session.csv")

    // Stop Spark Session
    spark.stop()
  }
}
