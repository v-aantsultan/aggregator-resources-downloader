package com.eci.common.services

import com.eci.common.{SharedBaseTest, TestSparkSession}
import org.apache.spark.sql.functions.to_timestamp

class DateTimeServiceTest extends SharedBaseTest with TestSparkSession {
  private val dateTimeService = new DateTimeService(testSparkSession)
  import testSparkSession.implicits._

  "dateTimeService" should "return array of distinct date" in {
    // 2021-12-09 8AM, 2021-12-09 11AM, 2021-12-10 1AM, 2021-12-10 11PM
    val data = Seq(1639037433, 1639048233, 1639098633, 1639177833)
    val expectedOutput = Array("20211209", "20211210", "20211211")

    val rdd = testSparkSession.sparkContext.parallelize(data)
    val df = rdd.toDF("timestamp_long")
      .select(to_timestamp($"timestamp_long").as("timestamp"))
    val resDf = dateTimeService.getDateArray(df, 3, "hours")

    resDf shouldBe expectedOutput
  }

  "dateTimeService (2 days timespan)" should "return array of distinct date" in {
    // 2021-12-09 8AM, 2021-12-09 11AM, 2021-12-23 9AM
    val data = Seq(1639037433, 1639048233, 1640252689)
    val expectedOutput = Array("20211207", "20211208", "20211209", "20211210", "20211211",
      "20211221", "20211222", "20211223", "20211224", "20211225")

    val rdd = testSparkSession.sparkContext.parallelize(data)
    val df = rdd.toDF("timestamp_long")
      .select(to_timestamp($"timestamp_long").as("timestamp"))
    val resDf = dateTimeService.getDateArray(df, 2, "days")

    resDf shouldBe expectedOutput
  }
}
