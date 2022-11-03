package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.anaplan.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar.mock

class SlpCsf01DFTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession {

  private val mockS3SourceService : S3SourceService  = mock[S3SourceService]

  before {
    when(mockS3SourceService.SlpCsf01Src).thenReturn(mockSlpCsf01Src)
    when(mockS3SourceService.MappingUnderLyingProductSrc).thenReturn(mockMappingUnderlyingProductSrc)
  }

  private val slpCsf01DF : SlpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)


  "IC Paylater data" should "only contain valid columns" in {
    val resDf = slpCsf01DF.getSpecific
    val validationColumn = Array(
      "report_date",
      "source_of_fund",
      "installment_plan",
      "no_of_transactions",
      "gmv",
      "admin_fee_commission",
      "interest_amount",
      "mdr_fee",
      "service_income",
      "user_acquisition_fee"
    )

    val resColumns = resDf.columns
    resColumns shouldBe validationColumn
  }

  "data" should "show" in {
    slpCsf01DF.getSpecific.show()
  }

  "data left join" should "show" in {
    slpCsf01DF.getJoinTable.show()
  }

  "count data" should "show" in {
    val countData = slpCsf01DF.getSpecific.count()
    println(s"countData : $countData")
  }
}
