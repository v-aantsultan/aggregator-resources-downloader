package com.eci.anaplan

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
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
  protected val mockSlpCsf01Src : DataFrame =
    testSparkSession.read.parquet(s"$MockResourcePath/slp_csf/csf_01/report_date=**")

  protected val mockMappingUnderlyingProductSrc : DataFrame =
    testSparkSession.read.parquet(s"$MockResourcePath/eci_sheets/ecidtpl_anaplan_fpna/Mapping Underlying Product")

  // protected val adjustmentDf = testSparkSession.read.parquet(s"$MockResourcePath/$tbiContext/adjustment/date=**")

  // protected val busAdjAdapterTestDataPath = s"$mockJoinerResourcePath/bus_adj_adapter.csv"
}
