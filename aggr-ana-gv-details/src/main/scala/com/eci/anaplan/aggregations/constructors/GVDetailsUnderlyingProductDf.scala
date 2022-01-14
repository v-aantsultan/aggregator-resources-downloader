package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVDetailsSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class GVDetailsUnderlyingProductDf @Inject()(val sparkSession: SparkSession, s3SourceService: GVDetailsSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.UnderlyingProductDf
      .select(
        $"`fs_product_type`".as("fs_product_type"),
        $"`underlying_product`".as("underlying_product")
      )
  }
}