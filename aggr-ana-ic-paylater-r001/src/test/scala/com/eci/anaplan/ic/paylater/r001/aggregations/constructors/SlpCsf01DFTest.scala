package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.anaplan.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar.mock

class SlpCsf01DFTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession {

  private val mockS3SourceService : S3SourceService  = mock[S3SourceService]
  private var slpCsf01DF : SlpCsf01DF = _

  before {
    when(mockS3SourceService.getSlpCsf01Src(true)).thenReturn(getMockSlpCsf01Src())
    when(mockS3SourceService.getMappingUnderLyingProductSrc(true)).thenReturn(getMockMappingUnderlyingProductSrc())
    slpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)
  }

  "IC Paylater data" should "only contain valid columns" in {
    val resDf = slpCsf01DF.getJoinTable
    val validationColumn = Array(
      "report_date",
      "product_category",
      "source_of_fund",
      "installment_plan",
      "no_of_transactions",
      "gmv",
      "admin_fee_commission",
      "interest_amount",
      "mdr_fee",
      "service_income",
      "user_acquisition_fee",
      "product"
    )

    val resColumns = resDf.columns
    resColumns shouldBe validationColumn
  }

  it should "not 0" in {
    val countData = slpCsf01DF.getSpecific.count()
    assert(countData != 0)
  }

}
