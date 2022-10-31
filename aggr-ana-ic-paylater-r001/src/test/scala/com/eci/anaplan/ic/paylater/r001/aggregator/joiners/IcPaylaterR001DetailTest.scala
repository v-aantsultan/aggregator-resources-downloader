package com.eci.anaplan.ic.paylater.r001.aggregator.joiners

import com.eci.anaplan.ic.paylater.r001.aggregations.constructors.IcPaylaterR001DF
import com.eci.anaplan.ic.paylater.r001.aggregations.joiners.IcPaylaterR001Detail
import com.eci.anaplan.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class IcPaylaterR001DetailTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession{

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]

  before {
    Mockito.when(mockS3SourceService.IcPaylaterCsf01Src).thenReturn(mockIcPaylaterR001Csf01)
  }

  private val icPaylaterR001DF: IcPaylaterR001DF = new IcPaylaterR001DF(testSparkSession, mockS3SourceService)
  private val icPaylaterR001Detail: IcPaylaterR001Detail = new IcPaylaterR001Detail(testSparkSession, icPaylaterR001DF)

  "Data" should "show" in {
    icPaylaterR001Detail.joinWithColumn().show()
  }
}
