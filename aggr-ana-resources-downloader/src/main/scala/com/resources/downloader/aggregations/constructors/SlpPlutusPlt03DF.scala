package com.resources.downloader.aggregations.constructors

import com.eci.common.aggregations.constructors.ConstructorsTrait
import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class SlpPlutusPlt03DF @Inject()(
                                val sparkSession: SparkSession,
                                s3SourceService: S3SourceService
                                ) extends ConstructorsTrait{

  import sparkSession.implicits._

  override def getSpecific: DataFrame = {
    s3SourceService.getSlpPlutusPlt03Src(false)
//      .drop("merchant_payment_request_id")
      .select("*")
  }
}
