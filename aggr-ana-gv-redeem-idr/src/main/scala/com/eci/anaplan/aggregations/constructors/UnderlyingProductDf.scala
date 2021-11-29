package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class UnderlyingProductDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.UnderlyingProductDf
      .select(
        $"`fs_product_type`".as("fs_product_type"),
        $"`underlying_product`".as("underlying_product")
      )
  }
}