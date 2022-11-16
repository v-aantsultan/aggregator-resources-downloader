package com.eci.anaplan.ic.paylater.r003.aggregations.constructors

import com.eci.anaplan.ic.paylater.r003.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar.mock

class SlpCsf03DFTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession{

  private val mockS3SourceService: S3SourceService = mock[S3SourceService]

  before {
    when(mockS3SourceService.getSlpCsf03Src(false)).thenReturn(getMockSlpCsf03Src())
  }

  private val slpCsf03DF: SlpCsf03DF = new SlpCsf03DF(testSparkSession, mockS3SourceService)

  "IC Paylater data" should "only contain valid columns" in {
    val resDf = slpCsf03DF.getSpecific
    val validationColumn = Array(
      "report_date",
      "product_category",
      "source_of_fund",
      "transaction_type",
      "loan_disbursed"
    )

    val resColumns = resDf.columns
    resColumns shouldBe validationColumn
  }

  it should "not 0" in {
    val countData = slpCsf03DF.getSpecific.count()
    assert(countData != 0)
  }

  it should "show" in {
    slpCsf03DF.getSpecific.show()
  }
}
