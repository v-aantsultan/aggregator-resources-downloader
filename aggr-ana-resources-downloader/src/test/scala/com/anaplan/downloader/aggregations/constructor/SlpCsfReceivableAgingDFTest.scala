package com.anaplan.downloader.aggregations.constructor

import com.anaplan.downloader.SharedBaseTest
import com.eci.common.services.S3SourceService
import com.resources.downloader.aggregations.constructors.SlpCsfReceivableAgingDF
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class SlpCsfReceivableAgingDFTest extends SharedBaseTest{

  val s3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  private var slpCsfReceivableAgingDF: SlpCsfReceivableAgingDF = _
  before {
    Mockito.when(s3SourceService.SlpCsfReceivableAgingSrc).thenReturn(mockSlpCsfReceivableAging)
    slpCsfReceivableAgingDF = new SlpCsfReceivableAgingDF(testSparkSession, s3SourceService)
  }

  it should "show" in {
    slpCsfReceivableAgingDF.getSpecific.printSchema()
  }

}
