package com.eci.anaplan.ic.paylater.r003.aggregations.joiners

import com.eci.anaplan.ic.paylater.r003.aggregations.constructors._
import com.eci.anaplan.ic.paylater.r003.{SharedBaseTest, SharedDataFrameStubber}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class IcPaylaterR003DetailTest extends SharedBaseTest with SharedDataFrameStubber {

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  var icPaylaterR003Detail: IcPaylaterR003Detail = _

  before {
    Mockito.when(mockS3SourceService.getSlpCsf01Src(true)).thenReturn(getMockSlpCsf01Src())
    Mockito.when(mockS3SourceService.getSlpCsf03Src(false, false)).thenReturn(getMockSlpCsf03Src(false))
    Mockito.when(mockS3SourceService.getSlpCsf07Src(false, false)).thenReturn(getMockSlpCsf07Src(false))
    Mockito.when(mockS3SourceService.getSlpPlutusPlt01Src(false)).thenReturn(getMockSlpPlutusPlt01Src())
    Mockito.when(mockS3SourceService.getSlpPlutusPlt03Src(false)).thenReturn(getMockSlpPlutusPlt03Src())
    Mockito.when(mockS3SourceService.getSlpPlutusPlt07Src(false)).thenReturn(getMockSlpPlutusPlt07Src())
    val slpCsf01Df = new SlpCsf01DF(testSparkSession, mockS3SourceService)
    val slpCsf03Df = new SlpCsf03DF(testSparkSession, mockS3SourceService)
    val slpCsf07Df = new SlpCsf07DF(testSparkSession, mockS3SourceService)
    val slpPlutusPlt01 = new SlpPlutusPlt01DF(testSparkSession, mockS3SourceService)
    val slpPlutusPlt03 = new SlpPlutusPlt03DF(testSparkSession, mockS3SourceService)
    val slpPlutusPlt07 = new SlpPlutusPlt07DF(testSparkSession, mockS3SourceService)

    testSparkSession.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    icPaylaterR003Detail = new IcPaylaterR003Detail(
      testSparkSession, slpCsf01Df, slpCsf03Df, slpCsf07Df, slpPlutusPlt01, slpPlutusPlt03, slpPlutusPlt07
    )
  }

  it should "only contain valid columns" in {
    val dataColumn = icPaylaterR003Detail.joinWithColumn().columns
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
    val countData = icPaylaterR003Detail.joinWithColumn().count()
    assert(countData != 0)
  }
}
