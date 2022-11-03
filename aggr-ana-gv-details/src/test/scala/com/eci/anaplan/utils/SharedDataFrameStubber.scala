package com.eci.anaplan.utils

/**
 * Reads Stub value from Parquet files in src/test/resources
 */
trait SharedDataFrameStubber extends TestSparkSession {

  val ResourcePath = "MockResource"
  val MockResourcePath = getClass.getResource(s"/$ResourcePath").getPath

  // TODO: Add all TestDataFrame here. All the mocked data frame will mock the Dataframes from dataFrameSource1
  protected val mockedExchangeRateSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/oracle.exchange_rates/conversion_date_date=**")
  protected val mockedUnderlyingProductSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/eci_sheets/ecidtpl_anaplan_fpna/Mapping Underlying Product")
  protected val mockedInvoiceSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/ecbpdf/payment_in_data_fetcher.invoice/created_at_date=**")
  protected val mockedPaymentSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/ecbpdf/payment_in_data_fetcher.payment/created_at_date=**")
  protected val mockedPaymentMDRSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/ecbpdf/payment_in_data_fetcher.payment_mdr_acquiring/created_at_date=**")
  protected val mockedGVIssuedSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/gift_voucher.gv_issuedlist/issued_date_date=**")
  protected val mockedGVRedeemedSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/gift_voucher.gv_redeemed/redemption_date_date=**")
  protected val mockedGVRevenueSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/gift_voucher.gv_revenue/revenue_date_date=**")
  protected val mockedGVSalesB2BSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/gift_voucher.gv_movement/report_date_date=**")
  protected val mockedGVSalesB2CSrc =
    testSparkSession.read.parquet(s"$MockResourcePath/gift_voucher.gv_sales/issued_date_date=**")
  /**
   * TODO: Add Other DataFrame
   */
}
