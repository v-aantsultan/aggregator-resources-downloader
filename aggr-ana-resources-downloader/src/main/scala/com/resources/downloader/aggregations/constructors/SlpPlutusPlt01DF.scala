package com.resources.downloader.aggregations.constructors

import com.eci.common.aggregations.constructors.ConstructorsTrait
import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, sum}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpPlutusPlt01DF @Inject() (
                                 val sparkSession: SparkSession,
                                 s3SourceService: S3SourceService
                                 ) extends ConstructorsTrait {

  override def getSpecific: DataFrame = {
    s3SourceService.SlpPlutusPlt01Src.select("*")
  }
}
