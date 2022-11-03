package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.aggregations.joiners.{AnaplanLoyaltyPointDtl, LPDetailsIDR, LoyaltyPointDtlPrep}
import com.eci.anaplan.services.LPDetailsSource
import com.eci.anaplan.utils.{SharedBaseTest, TestSparkSession}
import org.mockito.Mockito.{mock, when}

/**
 * This is a sample Test. It will not run as the mocked parquet file doesn't exist
 * TODO: Rename the class and update the test like the ones in the block comment
 */
class TestDataFrame1Test extends SharedBaseTest with TestSparkSession {
  import testSparkSession.implicits._

  private val mockS3SourceService: LPDetailsSource = mock(classOf[LPDetailsSource])
  private val testLPDetailsGrandProductTypeDf: LPDetailsGrandProductTypeDf =
    new LPDetailsGrandProductTypeDf(testSparkSession, mockS3SourceService)
  private val testLPDetailsTransactionCategoryDf: LPDetailsTransactionCategoryDf =
    new LPDetailsTransactionCategoryDf(testSparkSession, mockS3SourceService)
  private val testLPDetailsUnderlyingProductDf: LPDetailsUnderlyingProductDf =
    new LPDetailsUnderlyingProductDf(testSparkSession, mockS3SourceService)
  private val testLPDetailsRateDf: LPDetailsRateDf = new LPDetailsRateDf(testSparkSession, mockS3SourceService)
  private val testLPDetailsDf: LPDetailsDf = new LPDetailsDf(testSparkSession, mockS3SourceService)

  before {
    when(mockS3SourceService.GrandProductTypeDf).thenReturn(mockedGrantProductSrc)
    when(mockS3SourceService.TransactionCategoryDf).thenReturn(mockedTransactionCategorySrc)
    when(mockS3SourceService.underlyingProductDf).thenReturn(mockedUnderlyingProductSrc)
    when(mockS3SourceService.ExchangeRateDf).thenReturn(mockedExchangeRateSrc)
    when(mockS3SourceService.LPMutationDf).thenReturn(mockedLPMutationSrc)
  }

  private val testLPDetailsIDR: LPDetailsIDR =
    new LPDetailsIDR(testSparkSession, testLPDetailsDf, testLPDetailsRateDf,
      testLPDetailsGrandProductTypeDf, testLPDetailsTransactionCategoryDf, testLPDetailsUnderlyingProductDf)
  private val testLoyaltyPointDtlPrep: LoyaltyPointDtlPrep =
    new LoyaltyPointDtlPrep(testSparkSession, testLPDetailsIDR)
  private val testAnaplanLoyaltyPointDtl: AnaplanLoyaltyPointDtl =
    new AnaplanLoyaltyPointDtl(testSparkSession, testLoyaltyPointDtlPrep)

  "Loyalty Point Details DataFrame get" should "only contain valid columns" in {
    val resDf = testAnaplanLoyaltyPointDtl.joinWithColumn()
    val validExpectedColumns = Array(
      "report_date",
      "category",
      "customer",
      "product_category",
      "point_transaction",
      "point_amount"
    )

    val resColumns = resDf.columns
    resColumns shouldBe validExpectedColumns
  }
}
