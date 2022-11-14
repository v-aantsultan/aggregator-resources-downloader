package com.eci.anaplan.ic.paylater.r003.aggregations.constructors

import com.eci.anaplan.ic.paylater.r003.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class SlpCsf07DFTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession {

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  private var slpCsf07DF: SlpCsf07DF = _

  before {
    Mockito.when(mockS3SourceService.SlpCsf07Src).thenReturn(getMockSlpCsf07Src())
    slpCsf07DF = new SlpCsf07DF(testSparkSession, mockS3SourceService)
  }

  "IC Paylater data" should "only contain valid columns" in {
    val resDf = slpCsf07DF.getSpecific
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
    val countData = slpCsf07DF.getSpecific.count()
    assert(countData != 0)
  }

  it should "show table" in {
    slpCsf07DF.getSpecific.show()
  }
}
