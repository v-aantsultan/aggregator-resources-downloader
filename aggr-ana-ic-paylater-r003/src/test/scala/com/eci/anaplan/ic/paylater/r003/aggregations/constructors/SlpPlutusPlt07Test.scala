package com.eci.anaplan.ic.paylater.r003.aggregations.constructors

import com.eci.anaplan.ic.paylater.r003.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class SlpPlutusPlt07Test extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession{

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  private var slpPlutusPlt07DF: SlpPlutusPlt07DF = _

  before {
    Mockito.when(mockS3SourceService.getSlpPlutusPlt07Src(false)).thenReturn(getMockSlpPlutusPlt07Src())
    slpPlutusPlt07DF = new SlpPlutusPlt07DF(testSparkSession, mockS3SourceService)
  }

  "IC Paylater data" should "only contain valid columns" in {
    val resDf = slpPlutusPlt07DF.getSpecific
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
    val countData = slpPlutusPlt07DF.getSpecific.count()
    println(s"count data: $countData")
    assert(countData != 0)
  }

}
