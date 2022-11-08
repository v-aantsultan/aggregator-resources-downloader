package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.anaplan.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar.mock

class SlpCsf01DFTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession {

  private val mockS3SourceService : S3SourceService  = mock[S3SourceService]

  before {
    when(mockS3SourceService.SlpCsf01Src).thenReturn(getMockSlpCsf01Src())
    when(mockS3SourceService.MappingUnderLyingProductSrc).thenReturn(getMockMappingUnderlyingProductSrc())
  }

  private val slpCsf01DF : SlpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)


  "IC Paylater data" should "only contain valid columns" in {
    val resDf = slpCsf01DF.getJoinTable
    val validationColumn = Array(
      "report_date",
      "source_of_fund",
      "funding",
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
    println(s"count data : $countData")
    assert(countData != 0)
  }

  "data" should "show" in {
//    slpCsf01DF.getSpecific.orderBy(functions.col("report_date").asc).show(5)
    slpCsf01DF.getSpecific.show()
  }

  "data left join" should "show" in {
    slpCsf01DF.getJoinTable.show(5)
  }

  it should "show report_date schema" in {
    val dataType = slpCsf01DF.getSpecific.schema("report_date").dataType
    println(s"data type : $dataType")
  }

}
