package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVRedeemSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class GVRedeemUnderlyingProductDf @Inject()(val sparkSession: SparkSession, s3SourceService: GVRedeemSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.UnderlyingProductDf
      .select(
        $"`fs_product_type`".as("fs_product_type"),
        $"`underlying_product`".as("underlying_product")
      )
  }
}