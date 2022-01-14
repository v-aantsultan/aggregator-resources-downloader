package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.LPDetailsSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class LPDetailsUnderlyingProductDf @Inject()(val sparkSession: SparkSession, s3SourceService: LPDetailsSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.underlyingProductDf
      .select(
        $"`fs_product_type`".as("fs_product_type"),
        $"`underlying_product`".as("underlying_product")
      )
  }
}