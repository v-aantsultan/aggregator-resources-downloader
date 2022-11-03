package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.aggregations.joiners.{AnaplanLoyaltyPointSum, LPSummaryIDR, LoyaltyPointSumPrep}
import com.eci.anaplan.services.LPSummarySource
import com.eci.anaplan.utils.{SharedBaseTest, TestSparkSession}
import org.mockito.Mockito.{mock, when}

/**
 * This is a sample Test. It will not run as the mocked parquet file doesn't exist
 * TODO: Rename the class and update the test like the ones in the block comment
 */
class TestDataFrame1Test extends SharedBaseTest with TestSparkSession {
  import testSparkSession.implicits._

  private val mockS3SourceService: LPSummarySource = mock(classOf[LPSummarySource])
  private val testLPSummaryGrandProductTypeDf: LPSummaryGrandProductTypeDf =
    new LPSummaryGrandProductTypeDf(testSparkSession, mockS3SourceService)
  private val testLPSummaryTransactionCategoryDf: LPSummaryTransactionCategoryDf =
    new LPSummaryTransactionCategoryDf(testSparkSession, mockS3SourceService)
  private val testLPSummaryUnderlyingProductDf: LPSummaryUnderlyingProductDf =
    new LPSummaryUnderlyingProductDf(testSparkSession, mockS3SourceService)
  private val testLPSummaryRateDf: LPSummaryRateDf = new LPSummaryRateDf(testSparkSession, mockS3SourceService)
  private val testLPSummaryDf: LPSummaryDf = new LPSummaryDf(testSparkSession, mockS3SourceService)

  before {
    when(mockS3SourceService.GrandProductTypeDf).thenReturn(mockedGrantProductSrc)
    when(mockS3SourceService.TransactionCategoryDf).thenReturn(mockedTransactionCategorySrc)
    when(mockS3SourceService.underlyingProductDf).thenReturn(mockedUnderlyingProductSrc)
    when(mockS3SourceService.ExchangeRateDf).thenReturn(mockedExchangeRateSrc)
    when(mockS3SourceService.LPMutationDf).thenReturn(mockedLPMutationSrc)
  }

  private val testLPSummaryIDR: LPSummaryIDR =
    new LPSummaryIDR(testSparkSession, testLPSummaryDf, testLPSummaryRateDf,
      testLPSummaryGrandProductTypeDf, testLPSummaryTransactionCategoryDf, testLPSummaryUnderlyingProductDf)
  private val testLoyaltyPointSumPrep: LoyaltyPointSumPrep =
    new LoyaltyPointSumPrep(testSparkSession, testLPSummaryIDR)
  private val testAnaplanLoyaltyPointSum: AnaplanLoyaltyPointSum =
    new AnaplanLoyaltyPointSum(testSparkSession, testLoyaltyPointSumPrep)

  "Loyalty Point Summary DataFrame get" should "only contain valid columns" in {
    val resDf = testAnaplanLoyaltyPointSum.joinWithColumn()
    val validExpectedColumns = Array(
      "report_date",
      "customer",
      "selling_point_transactions",
      "selling_point_amount",
      "employee_benefit_points_transactions",
      "employee_benefit_points_amount",
      "point_catalogue_transactions",
      "point_catalogue_amount",
      "discount",
      "expired"
    )

    val resColumns = resDf.columns
    resColumns shouldBe validExpectedColumns
  }
}
