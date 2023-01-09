package com.resources.downloader.aggregations.constructors

import com.eci.common.aggregations.constructors.ConstructorsTrait
import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpCsf03DF @Inject()(
                          val sparkSession: SparkSession,
                          s3SourceService: S3SourceService
                          ) extends ConstructorsTrait{

  import sparkSession.implicits._

  private lazy val slpCsf03DF = s3SourceService.getSlpCsf03Src(false, false)

  override def getSpecific: DataFrame = {
    slpCsf03DF.select("*")
  }
}
