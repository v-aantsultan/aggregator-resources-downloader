package com.eci.anaplan.bs.details.utils

/**
 * Reads Stub value from Parquet files in src/test/resources
 */
trait SharedDataFrameStubber extends TestSparkSession {
  val testResourcePath = getClass.getResource("/").getPath

  // TODO: Add all TestDataFrame here. All the mocked data frame will mock the Dataframes from dataFrameSource1
  protected val mockedDataFrameSource1 = testSparkSession.read.parquet(s"$testResourcePath/TestDataFrame/date=**")
  /**
   * TODO: Add Other DataFrame
   */
}
