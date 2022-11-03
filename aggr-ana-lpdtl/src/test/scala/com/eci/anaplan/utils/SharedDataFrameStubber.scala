package com.eci.anaplan.utils

/**
 * Reads Stub value from Parquet files in src/test/resources
 */
trait SharedDataFrameStubber extends TestSparkSession {
  val ResourcePath = "MockResource"
  val MockResourcePath = getClass.getResource(s"/$ResourcePath").getPath

  // TODO: Add all TestDataFrame here. All the mocked data frame will mock the Dataframes from dataFrameSource1
  protected val mockedGrantProductSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/eci_sheets/ecidtpl_anaplan_fpna/Mapping Grant Product Type")
  protected val mockedTransactionCategorySrc =
    testSparkSession.read.parquet(s"$MockResourcePath/eci_sheets/ecidtpl_anaplan_fpna/Mapping Transaction Category")
  protected val mockedUnderlyingProductSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/eci_sheets/ecidtpl_anaplan_fpna/Mapping Underlying Product")
  protected val mockedExchangeRateSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/oracle.exchange_rates/conversion_date_date=**")
  protected val mockedLPMutationSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/loyalty_point.point_mutation/movement_time_date=**")
  /**
   * TODO: Add Other DataFrame
   */
}
