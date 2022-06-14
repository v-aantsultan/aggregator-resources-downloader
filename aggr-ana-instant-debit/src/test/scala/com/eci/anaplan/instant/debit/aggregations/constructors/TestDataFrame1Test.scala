package com.eci.anaplan.instant.debit.aggregations.constructors

import com.eci.anaplan.instant.debit.services.S3SourceService
import com.eci.anaplan.instant.debit.utils.{SharedBaseTest, TestSparkSession}
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar.mock

/**
 * This is a sample Test. It will not run as the mocked parquet file doesn't exist
 * TODO: Rename the class and update the test like the ones in the block comment
 */
class TestDataFrame1Test extends SharedBaseTest with TestSparkSession {

  private val mockS3SourceService: S3SourceService = mock[S3SourceService]
  private val testExchangeRateDf: ExchangeRateDf = new ExchangeRateDf(testSparkSession, mockS3SourceService)
  before {
    when(mockS3SourceService.dataFrameSource1).thenReturn(mockedDataFrameSource1)
  }

  /*
  // TODO :  Write Some test Like this

  "Exchange Rates DataFrame get" should "only contain valid columns" in {
    val resDf = testExchangeRateDf.get
    val validExpectedColumns = Array(
      "from_currency",
      "to_currency",
      "conversion_date",
      "conversion_type",
      "conversion_rate"
    )

    val resColumns = resDf.columns
    resColumns shouldBe validExpectedColumns
  }
  */
}
