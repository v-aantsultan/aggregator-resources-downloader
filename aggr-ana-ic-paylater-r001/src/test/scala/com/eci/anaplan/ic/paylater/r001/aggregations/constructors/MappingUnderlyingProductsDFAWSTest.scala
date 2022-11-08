package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.anaplan.ic.paylater.r001.config.ConfigTest

class MappingUnderlyingProductsDFAWSTest extends ConfigTest{

  "get mapping underlying product data lake or data warehouse from aws" should "run" in {
    s3SourceService.MappingUnderLyingProductSrc.show()
  }
}
