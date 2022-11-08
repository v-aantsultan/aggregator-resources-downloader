package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.anaplan.ic.paylater.r001.config.ConfigTest

class SlpCsf01DFAWSTest extends ConfigTest{

  "data from aws" should "show" in {
    val slpCsf01DF: SlpCsf01DF = new SlpCsf01DF(sparkSession, s3SourceService)
    slpCsf01DF.getSpecific.show()
  }

  "get slp_csf csf_01 data lake or data warehouse from aws" should "run" in {
    // s3SourceService.SlpCsf01Src.select("*").show()
    s3SourceService.SlpCsf01Src
  }
}
