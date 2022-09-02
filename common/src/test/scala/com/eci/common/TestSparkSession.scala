package com.eci.common

import org.apache.spark.sql.SparkSession

trait TestSparkSession {
  // Refer to holden library the config used by him
  lazy val testSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  testSparkSession.sparkContext.setLogLevel("WARN")

  val testResourcePath = getClass.getResource("/").getPath

  val ResourcePath = "MockDatasource"
  val datalakeResourcePath = getClass.getResource(s"/$ResourcePath").getPath

  val bpContext: String = "business_platform"
  val bpMasterContext: String = "business_platform_master"
  val ecioraContext: String = "eciora"
  val ecbpdfContext: String = "ecbpdf/payment_in_data_fetcher"
  val dataWarehouseContext: String = "data_warehouse"

  val tbiContext: String = "tbi"

  // TODO: Add all TestDataFrame here. All the mocked data frame will mock the Dataframes from dataFrameSource1
  protected val mockedTrainSalesDataFrameSource =
    testSparkSession.read.parquet(s"$datalakeResourcePath/train.sales/purchase_delivery_timestamp_date=**")

//  protected val mockedAdjustmentDataFrameSource = testSparkSession.read.parquet(s"$datalakeResourcePath/$tbiContext/adjustment/date=**")

//  protected val mockedPaymentOutRequestDataFrameSource = testSparkSession.read.parquet(
//    s"$datalakeResourcePath/$bpContext/payment_out_request/date=**")
//
//  protected val mockedPayableInvoiceDataFrameSource = testSparkSession.read.parquet(
//    s"$datalakeResourcePath/$tbiContext/payable_invoice/date=**")
//
//  protected val mockedPurchaseInvoiceDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$bpContext/purchase_invoice/date=**")
//
//  protected val mockedOutgoingPaymentDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$bpContext/outgoing_payment/date=**")
//
//  protected val mockedExchangeRateDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$bpMasterContext/exchange_rate/date=**")
//
//  protected val mockedExchangeRateOracleDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$ecioraContext/exchange_rate/date=**")
//
//  protected val mockedBusinessPartnerDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$bpMasterContext/business_partner")
//
//  protected val mockedInvoiceDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$ecbpdfContext.invoice/created_at_date=**")
//
//  protected val mockedPaymentDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$ecbpdfContext.payment/created_at_date=**")
//
//  protected val mockedPaymentMDRDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$ecbpdfContext.payment_mdr_acquiring/created_at_date=**")
//
//  protected val mockedCurrencyDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$bpMasterContext/currency")
//
//  protected val mockedExchangeRateAvgDataFrameSource =
//    testSparkSession.read.parquet(s"$datalakeResourcePath/$ecioraContext/exchange_rate_avg/date=**")

}
