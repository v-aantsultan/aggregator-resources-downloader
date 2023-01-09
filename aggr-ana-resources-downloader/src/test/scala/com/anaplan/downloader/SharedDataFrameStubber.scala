package com.anaplan.downloader

/**
 * Reads Stub value from Parquet files in src/test/resources
 */
trait SharedDataFrameStubber extends TestSparkSession {

  val ResourcePath = "MockResources"
  val MockResourcePath = getClass.getResource(s"/$ResourcePath").getPath

  // TODO: Add all TestDataFrame here. All the mocked data frame will mock the Dataframes from dataFrameSource1

  val mockSlpCsfReceivableAging =
    testSparkSession.read
      .option("mergeSchema", true)
      .parquet(s"$MockResourcePath/slp_csf/csf_receivable_aging/report_date=**")

}
