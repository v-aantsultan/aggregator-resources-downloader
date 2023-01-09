package com.resources.downloader.aggregations.constructors

import com.eci.common.aggregations.constructors.ConstructorsTrait
import com.eci.common.services.S3SourceService
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpCsfReceivableAgingDF @Inject()(
                                       val sparkSession: SparkSession,
                                       s3SourceService: S3SourceService
                                       ) extends ConstructorsTrait{

  import sparkSession.implicits._

  override def getSpecific: DataFrame = {
    s3SourceService.SlpCsfReceivableAgingSrc
      .select("*")
  }
}
