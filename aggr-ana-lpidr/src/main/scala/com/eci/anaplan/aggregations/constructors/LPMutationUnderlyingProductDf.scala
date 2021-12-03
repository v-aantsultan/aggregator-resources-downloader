package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.LPMutationSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class LPMutationUnderlyingProductDf @Inject()(val sparkSession: SparkSession, s3SourceService: LPMutationSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.underlyingProductDf
      .select(
        $"`fs_product_type`".as("fs_product_type"),
        $"`underlying_product`".as("underlying_product")
      )
  }
}
