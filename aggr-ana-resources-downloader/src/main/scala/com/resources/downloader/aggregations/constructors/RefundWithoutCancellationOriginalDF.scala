package com.resources.downloader.aggregations.constructors

import com.eci.common.aggregations.constructors.ConstructorsTrait
import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class RefundWithoutCancellationOriginalDF @Inject()(
                                                   sparkSession: SparkSession,
                                                   s3SourceService: S3SourceService
                                                   ) extends ConstructorsTrait{
  override def getSpecific: DataFrame = {
    s3SourceService.RefundWithoutCancellationOriginalSrc
      .select("*")
  }
}
