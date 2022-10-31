package com.eci.anaplan.ic.paylater.r001.aggregator.constructors

import com.eci.anaplan.ic.paylater.r001.aggregations.constructors.IcPaylaterR001DF
import com.eci.anaplan.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.scalatest.mockito.MockitoSugar.mock
import org.mockito.Mockito.when

class IcPaylaterR001DFTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession {

  private val mockS3SourceService : S3SourceService  = mock[S3SourceService]

  before {
    when(mockS3SourceService.IcPaylaterCsf01Src).thenReturn(mockIcPaylaterR001Csf01)
  }

  private val icPaylaterR001DfMock : IcPaylaterR001DF = new IcPaylaterR001DF(testSparkSession, mockS3SourceService)


  "IC Paylater data" should "only contain valid columns" in {
    val resDf = icPaylaterR001DfMock.getAllPeriod
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
    icPaylaterR001DfMock.getAllPeriod.show()
  }

  "count data" should "show" in {
    val countData = icPaylaterR001DfMock.getAllPeriod.count()
    println(s"countData : $countData")
  }


}
