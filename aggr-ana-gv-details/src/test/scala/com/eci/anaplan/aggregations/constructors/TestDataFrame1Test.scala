package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.aggregations.joiners.AnaplanGiftVoucherDetails
import com.eci.anaplan.services.GVDetailsSource
import com.eci.anaplan.utils.{SharedBaseTest, TestSparkSession}
import org.mockito.Mockito.{mock, when}

/**
 * This is a sample Test. It will not run as the mocked parquet file doesn't exist
 * TODO: Rename the class and update the test like the ones in the block comment
 */
class TestDataFrame1Test extends SharedBaseTest with TestSparkSession {
  import testSparkSession.implicits._

  private val mockS3SourceService: GVDetailsSource = mock(classOf[GVDetailsSource])
  private val testGVDetailsRateDf: GVDetailsRateDf = new GVDetailsRateDf(testSparkSession, mockS3SourceService)
  private val testGVDetailsUnderlyingDf: GVDetailsUnderlyingProductDf =
    new GVDetailsUnderlyingProductDf(testSparkSession, mockS3SourceService)
  private val testGVDetailsMDRDf: GVDetailsMDRDf = new GVDetailsMDRDf(testSparkSession, mockS3SourceService)
  private val testGVIssuedIDRDf: GVIssuedIDRDf = new GVIssuedIDRDf(testSparkSession, mockS3SourceService)

  before {
    when(mockS3SourceService.ExchangeRateDf).thenReturn(mockedExchangeRateSrc)
    when(mockS3SourceService.UnderlyingProductDf).thenReturn(mockedUnderlyingProductSrc)
    when(mockS3SourceService.InvoiceDf).thenReturn(mockedInvoiceSrc)
    when(mockS3SourceService.PaymentDf).thenReturn(mockedPaymentSrc)
    when(mockS3SourceService.PaymentMDRDf).thenReturn(mockedPaymentMDRSrc)
    when(mockS3SourceService.GVIssuedlistDf).thenReturn(mockedGVIssuedSrc)
    when(mockS3SourceService.GVRedeemDf).thenReturn(mockedGVRedeemedSrc)
    when(mockS3SourceService.GVRevenueDf).thenReturn(mockedGVRevenueSrc)
    when(mockS3SourceService.GVSalesB2BDf).thenReturn(mockedGVSalesB2BSrc)
    when(mockS3SourceService.GVSalesB2CDf).thenReturn(mockedGVSalesB2CSrc)
  }

  private val testGVRedeemIDRDf: GVRedeemIDRDf =
    new GVRedeemIDRDf(testSparkSession, mockS3SourceService, testGVDetailsUnderlyingDf, testGVDetailsRateDf)
  private val testGVRevenueIDRDf: GVRevenueIDRDf = new GVRevenueIDRDf(testSparkSession, mockS3SourceService, testGVDetailsRateDf)
  private val testGVSalesB2BIDRDf: GVSalesB2BIDRDf = new GVSalesB2BIDRDf(testSparkSession, mockS3SourceService, testGVDetailsRateDf)
  private val testGVSalesB2CIDRDf: GVSalesB2CIDRDf = new
      GVSalesB2CIDRDf(testSparkSession, mockS3SourceService, testGVDetailsRateDf, testGVIssuedIDRDf, testGVDetailsMDRDf)
  private val testAnaplanGiftVoucherDetails: AnaplanGiftVoucherDetails =
    new AnaplanGiftVoucherDetails(testSparkSession, testGVRedeemIDRDf, testGVRevenueIDRDf, testGVSalesB2CIDRDf, testGVSalesB2BIDRDf)


  "Gift Voucher Details DataFrame get" should "only contain valid columns" in {
    val resDf = testAnaplanGiftVoucherDetails.joinWithColumn()
    val validExpectedColumns = Array(
      "report_date",
      "product",
      "business_partner",
      "voucher_redemption_product",
      "customer",
      "payment_channel_name",
      "gift_voucher_amount",
      "no_of_transactions",
      "no_gift_voucher",
      "revenue_amount",
      "unique_code",
      "coupon_value",
      "discount",
      "premium",
      "mdr_charges"
    )

    val resColumns = resDf.columns
    resColumns shouldBe validExpectedColumns
  }
}
