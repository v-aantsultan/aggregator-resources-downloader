package com.eci.anaplan.ic.paylater.r003

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
/**
 * Reads Stub value from Parquet files in src/test/resources
 */
trait SharedDataFrameStubber extends TestSparkSession {

  val ResourcePath = "MockResource"
  val MockResourcePath = getClass.getResource(s"/$ResourcePath").getPath

  // val JoinerPath = "MockJoiner"
  // val mockJoinerResourcePath = getClass.getResource(s"/$JoinerPath").getPath

//  val bpContext: String = "business_platform"
//  val bpMasterContext: String = "business_platform_master"
//  val tbiContext: String = "tbi"
//  val ecioraContext: String = "eciora"

  // val testResourcePath = getClass.getResource("/").getPath

  // TODO: Add all TestDataFrame here. All the mocked data frame will mock the Dataframes from dataFrameSource1
  protected def getMockSlpCsf01Src(): DataFrame = {
    this.getSparkSession(true).parquet(s"$MockResourcePath/slp_csf/csf_01/report_date=**")
  }

  protected def getMockSlpCsf03Src(): DataFrame = {
    this.getSparkSession(false).parquet(s"$MockResourcePath/slp_csf/csf_03/report_date=**")
  }

  protected def getMockSlpCsf07Src(): DataFrame = {
    this.getSparkSession(false).parquet(s"$MockResourcePath/slp_csf/csf_07/report_date=**")
  }

  protected def getMockSlpPlutusPlt01Src(): DataFrame = {
    this.getSparkSession(false).parquet(s"$MockResourcePath/slp_plutus/plt_01/report_date=**")
  }

  protected def getMockSlpPlutusPlt03Src(): DataFrame = {
    this.getSparkSession(false).parquet(s"$MockResourcePath/slp_plutus/plt_03/report_date=**")
  }

  protected def getMockSlpPlutusPlt07Src(): DataFrame = {
    this.getSparkSession(false).parquet(s"$MockResourcePath/slp_plutus/plt_07/report_date=**")
  }

  private def getSparkSession(isMergeSchema: Boolean): DataFrameReader = {
    testSparkSession
      .read
      .option("mergeSchema", isMergeSchema)
  }
}
