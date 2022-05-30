package raja.paypay.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.matching.Regex

object utils {

  /**
   * Pattern for parsing the Log.
   */
  val parserRegex: Regex = """(\S+) (\S+) (\S+):(\d+) (\S+):(\d+) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) (\d+) (\d+) "([^ ]*) ([^ ]*) (- |[^ ]*)" ("[^"]*") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$""".r


  // Initializing Required Variables

  val timeStampCol = "ts"
  val previousTimeStampCol = "previous_ts"
  val requestIpCol = "req_ip"
  val backendIpCol = "backend_ip"
  val timeSpendCol = "time_spend"
  val uniqueSessionFlagCol = "unique_ssn_flag"
  val sessionIdCol = "session_id"


  @transient lazy val spark: SparkSession = SparkSession.builder()
    .appName(" PayPayDEChallenge")
    .master("local[3]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  case class FullLogRecord(ts: String, domain_nm: String, req_ip: String, req_port: String
                           , backend_ip: String, backend_port: String, req_prcssng_time: String, backend_prcssng_time: String
                           , client_resp_tm: String, rea_resp_code: String, backend_resp_code: String, rcvd_bytes: String
                           , sent_bytes: String, req_type: String, url: String, protocol: String
                           , usr_agnt: String, ssl: String, ssl2: String
                          )

  case class ParsedLogRecord(ts: String, req_ip: String
                             , backend_ip: String, url: String
                            )

  def rawParserFunction(df: DataFrame, regex: Regex = parserRegex): DataFrame = {

    import spark.implicits._
    //    val logs = rawLog.filter(line=>line.matches(logRegex.toString)).map(line=>logMapping(line)).toDF()

    // Filter only the matching row as per RegEx.
    val filteredDF = df.filter(l => l.getString(0).matches(regex.toString()))
    val logDF = filteredDF.map(row => {
      row.getString(0) match {
        case regex(ts, domain_nm, req_ip, req_port
        , backend_ip, backend_port, req_prcssng_time, backend_prcssng_time
        , client_resp_tm, rea_resp_code, backend_resp_code, rcvd_bytes
        , sent_bytes, req_type, url, protocol
        , usr_agnt, ssl, ssl2) =>
          ParsedLogRecord(ts, req_ip, backend_ip, url) // Using case get only the required column for anlaytics.
      }
    }
    )
    logDF.toDF().repartition(4, col(requestIpCol)).cache()
  }

  def addingAdditionalCols(df: DataFrame, maxActiveSession: Int = 900): DataFrame = {

    // WIndow funciton on Req IP and Backend IP
    val windowFunc = Window.partitionBy(requestIpCol, backendIpCol)

    val windowTimeDiff = windowFunc.orderBy(col(timeStampCol).asc) // Order by TimeStamp to get Previous Ts
    val windowSession = windowFunc.orderBy(col(requestIpCol).asc, col(backendIpCol).asc, col(timeSpendCol).asc)

    val previousTs = lag(col(timeStampCol), 1).over(windowTimeDiff)

    val df_log = df.withColumn(timeStampCol, to_timestamp(col(timeStampCol)))
      .withColumn(previousTimeStampCol, previousTs) // Calculate Previous Time Stamp Col
      // Difference of current minus previous TimeStamp in Seconds
      .withColumn(timeSpendCol, unix_timestamp(col(timeStampCol)).minus(unix_timestamp(col(previousTimeStampCol))))
      .withColumn(uniqueSessionFlagCol, when(col(timeSpendCol) < lit(maxActiveSession), lit(0)).otherwise(lit(1)))
      // Calculate Session ID based on max active session.
      .withColumn(sessionIdCol, sum(col(uniqueSessionFlagCol)).over(windowSession))

    //Returned Parsed DF
    df_log
  }


  def fnSessionize(df: DataFrame): DataFrame = {
    //Group by IP and session ID and get distinct URLs
    val groupByCols = List(requestIpCol, sessionIdCol)
    val dfSessionize = df.groupBy(groupByCols.map(col): _*).agg(count("url").alias("ip_hits"))

    // Return Sessionized  DF

    dfSessionize
  }

  def fnAverageSessionTime(df: DataFrame): DataFrame = {

    // Group by Ip and Session ID and difference the max timestamp with min timestamp and get average across all
    val groupByCols = List(requestIpCol, sessionIdCol)
    val dfAvgSession = df.groupBy(groupByCols.map(col): _*).agg(
      max(timeStampCol).alias("max_ts"), min(timeStampCol).alias("min_ts")
    ).withColumn("session_time", unix_timestamp(col("max_ts")).minus(unix_timestamp(col("min_ts"))))
      .groupBy().agg(avg("session_time").alias("average_session_time"))

    // Return Average Session DF
    dfAvgSession
  }

  def fnUniqueUrlHits(df: DataFrame): DataFrame = {

    // Group By IP and Session and Get Unique URL page
    val groupByCols = List(requestIpCol, sessionIdCol)
    val dfUniqueUrl = df.groupBy(groupByCols.map(col): _*).agg(countDistinct("url").alias("unique_url_per_session"))

    // Return Unique URL DF

    dfUniqueUrl
  }

  def fnLongestSession(df: DataFrame): DataFrame = {
    // Group By IP and session id and get Time difference of start and end of the session and get max of time Spend
    val groupByCols = List(requestIpCol, sessionIdCol)
    val dfLongSession = df.groupBy(groupByCols.map(col): _*).agg(max(timeStampCol).alias("max_ts"), min(timeStampCol).alias("min_ts"))
      .withColumn("session_time", unix_timestamp(col("max_ts")).minus(unix_timestamp(col("min_ts"))))
      .groupBy("req_ip").agg(max(col("session_time")).alias("longest_session_per_ip"))

    // Return Longest Session DF.
    dfLongSession
  }

}