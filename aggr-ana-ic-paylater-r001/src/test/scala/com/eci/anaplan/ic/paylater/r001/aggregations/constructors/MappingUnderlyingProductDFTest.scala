package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.anaplan.{SharedBaseTest, SharedDataFrameStubber, TestSparkSession}
import com.eci.common.services.S3SourceService
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar.mock

class MappingUnderlyingProductDFTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession {

  private val mockS3SourceService : S3SourceService  = mock[S3SourceService]

  before {
    when(mockS3SourceService.MappingUnderLyingProductSrc).thenReturn(mockMappingUnderlyingProductSrc)
  }

  private val mappingUnderlyingProductDF : MappingUnderlyingProductDF =
    new MappingUnderlyingProductDF(testSparkSession, mockS3SourceService)

  it should "show" in {
    mappingUnderlyingProductDF.getData.show()
  }

  it should "iterate" in {

  }

}
