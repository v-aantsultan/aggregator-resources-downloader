package com.eci.anaplan.ic.paylater.r003.aggregations.constructors

import com.eci.anaplan.ic.paylater.r003.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class SlpPlutusPlt01Test extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession{

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  private var slpPlutusPlt01DF: SlpPlutusPlt01DF = _

  before {
    Mockito.when(mockS3SourceService.getSlpPlutusPlt01Src(false)).thenReturn(getMockSlpPlutusPlt01Src())
    slpPlutusPlt01DF = new SlpPlutusPlt01DF(testSparkSession, mockS3SourceService)
  }

  "IC Paylater data" should "only contain valid columns" in {
    val resDf = slpPlutusPlt01DF.getSpecific
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
    val countData = slpPlutusPlt01DF.getSpecific.count()
    assert(countData != 0)
  }
}
