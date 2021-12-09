package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.LPDetailsSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class LPDetailsGrandProductTypeDf @Inject()(val sparkSession: SparkSession, s3SourceService: LPDetailsSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.GrandProductTypeDf
      .select(
        $"`cost_type_id`".as("cost_type_id"),
        $"`grant_product_type`".as("grant_product_type")
      )
  }
}