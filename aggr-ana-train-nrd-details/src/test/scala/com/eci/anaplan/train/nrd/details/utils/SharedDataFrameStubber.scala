package com.eci.anaplan.train.nrd.details.utils

/**
 * Reads Stub value from Parquet files in src/test/resources
 */
trait SharedDataFrameStubber extends TestSparkSession {

  val ResourcePath = "MockResource"
  val MockResourcePath = getClass.getResource(s"/$ResourcePath").getPath

  // val JoinerPath = "MockJoiner"
  // val mockJoinerResourcePath = getClass.getResource(s"/$JoinerPath").getPath

  val bpContext: String = "business_platform"
  val bpMasterContext: String = "business_platform_master"
  val tbiContext: String = "tbi"
  val ecioraContext: String = "eciora"

  // val testResourcePath = getClass.getResource("/").getPath

  // TODO: Add all TestDataFrame here. All the mocked data frame will mock the Dataframes from dataFrameSource1
  protected val mockedTrainSalesAllPeriodSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/train.sales/purchase_delivery_timestamp_date=**")

  // protected val adjustmentDf = testSparkSession.read.parquet(s"$MockResourcePath/$tbiContext/adjustment/date=**")

  // protected val busAdjAdapterTestDataPath = s"$mockJoinerResourcePath/bus_adj_adapter.csv"
}