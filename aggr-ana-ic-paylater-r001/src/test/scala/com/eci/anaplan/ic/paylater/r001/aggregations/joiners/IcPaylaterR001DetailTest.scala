package com.eci.anaplan.ic.paylater.r001.aggregations.joiners

import com.eci.anaplan.ic.paylater.r001.aggregations.constructors.SlpCsf01DF
import com.eci.anaplan.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class IcPaylaterR001DetailTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession{

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  private var icPaylaterR001Detail: IcPaylaterR001Detail = _

  before {
    Mockito.when(mockS3SourceService.getSlpCsf01Src(true)).thenReturn(getMockSlpCsf01Src())
    Mockito.when(mockS3SourceService.getMappingUnderLyingProductSrc(true))
      .thenReturn(getMockMappingUnderlyingProductSrc())
    val slpCsf01DF: SlpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)
    icPaylaterR001Detail = new IcPaylaterR001Detail(testSparkSession, slpCsf01DF)
  }

  "Data's column" should "only contain valid columns" in {
    val dataColumn = icPaylaterR001Detail.joinWithColumn().columns
    val expectedColumn = Array (
      "report_date",
      "product_category",
      "source_of_fund",
      "installment_plan",
      // "loan_disbursed_value", TBD
      "product",
      "no_of_transactions",
      "gmv",
      "admin_fee_commission",
      "interest_amount",
      // "additional_interest", TBD
      "mdr_fee",
      "service_income",
      "user_acquisition_fee"
    )
    dataColumn shouldBe expectedColumn
  }

  "data" should "not 0" in {
    val countData = icPaylaterR001Detail.joinWithColumn().count()
    assert(countData != 0)
  }
}
