package raja.paypay.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class LogFileAnalyticsTest extends AnyFlatSpec with should.Matchers {

  // Read Sample Data from resource

  val spark: SparkSession = SparkSession.builder()
    .appName(" testPayPayDEChallenge")
    .master("local[2]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()


  //  val file = getClass.getResource(sampleDataPath).getPath
  val sampleDataPath = "/Users/r0m0831/other_git/DataEngineerChallenge/src/resources/unitTest.log"

  val rawSampleDF: DataFrame = spark.read.textFile(sampleDataPath).toDF()

  // Get RegEx Parsed Dataframe
  val parsedTestDF: DataFrame = utils.rawParserFunction(rawSampleDF)

  // Get Analytical Ready DF by adding calculative columns
  val maxActiveSsn = 900 // Set Max Active session time in seconds.
  val testDF: DataFrame = utils.addingAdditionalCols(parsedTestDF, maxActiveSsn)


  "fnAverageSessionTime function " should "match values for Sample data given " in {

    val dfA = utils.fnAverageSessionTime(testDF)
    // Check average session for sample file given is matching the expected value.
    val expectedAverageSession = 0.0797872340425532
    assert(dfA.first()(0) == expectedAverageSession)
  }

  "fnUniqueUrlHits function " should "match expected values for Sample data given " in {

    val dfURLhits = utils.fnUniqueUrlHits(testDF)
    val testIp = "1.39.32.136"
    val expectedURLhits = 2

    // Check unique_url_per_session for testIP in sample file is matching expected Value
    val actualURLhits = dfURLhits.filter(col(utils.requestIpCol) === testIp).select("unique_url_per_session").first()(0)
    assert(actualURLhits == expectedURLhits)
  }

  "fnLongestSession function " should "match expected values for Sample data given " in {

    val dfLongstSsn = utils.fnLongestSession(testDF)
    val testIp = "203.92.217.105"

    // Check longest_session_per_ip for testIP in sample file is matching expected Value
    val expectedLongstSession = 1
    val actualLongstSession = dfLongstSsn.filter(col(utils.requestIpCol) === testIp).select("longest_session_per_ip").first()(0)
    assert(expectedLongstSession == actualLongstSession)
  }

}