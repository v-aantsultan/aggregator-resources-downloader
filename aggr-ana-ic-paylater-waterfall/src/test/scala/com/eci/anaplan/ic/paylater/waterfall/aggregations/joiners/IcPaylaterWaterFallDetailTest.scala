package com.eci.anaplan.ic.paylater.waterfall.aggregations.joiners

import com.eci.anaplan.ic.paylater.waterfall.aggregations.constructors.{SlpCsf01DF, SlpCsf03DF, SlpCsf07DF}
import com.eci.anaplan.ic.paylater.waterfall.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.apache.spark.sql.functions
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class IcPaylaterWaterFallDetailTest extends SharedBaseTest with SharedDataFrameStubber {

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  var icPaylaterWaterFallDetail: IcPaylaterWaterFallDetail = _

  before {
    Mockito.when(mockS3SourceService.SlpCsf01Src).thenReturn(getMockSlpCsf01Src())
    Mockito.when(mockS3SourceService.SlpCsf03Src).thenReturn(getMockSlpCsf03Src())
    Mockito.when(mockS3SourceService.SlpCsf07Src).thenReturn(getMockSlpCsf07Src())
    val slpCsf01DF: SlpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)
    val slpCsf03DF: SlpCsf03DF = new SlpCsf03DF(testSparkSession, mockS3SourceService)
    val slpCsf07DF: SlpCsf07DF = new SlpCsf07DF(testSparkSession, mockS3SourceService)
    icPaylaterWaterFallDetail = new IcPaylaterWaterFallDetail(testSparkSession, slpCsf01DF, slpCsf03DF, slpCsf07DF)
  }

  it should "only contain valid columns" in {
    val dataColumn = icPaylaterWaterFallDetail.joinWithColumn().columns
    val expectedColumn = Array (
      "report_date",
      "product_category",
      "source_of_fund",
      "transaction_type",
      "loan_disbursed"
    )
    dataColumn shouldBe expectedColumn
  }

  it should "not 0" in {
    val countData = icPaylaterWaterFallDetail.joinWithColumn().count()
    println(s"count data : $countData")
    assert(countData != 0)
  }

  it should "show result" in {
    icPaylaterWaterFallDetail.joinWithColumn()
      .orderBy(functions.col("report_date").asc, functions.col("transaction_type").asc).show()
  }
}
