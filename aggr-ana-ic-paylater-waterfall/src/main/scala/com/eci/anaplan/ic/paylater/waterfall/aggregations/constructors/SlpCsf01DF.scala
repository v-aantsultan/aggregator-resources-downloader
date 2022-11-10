package com.eci.anaplan.ic.paylater.waterfall.aggregations.constructors

import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpCsf01DF @Inject()(
                          val sparkSession: SparkSession,
                          s3SourceService: S3SourceService
                          ) {

  import sparkSession.implicits._

  def getSpecific: DataFrame = {
    s3SourceService.SlpCsf01Src
      .select($"*")
  }

}
