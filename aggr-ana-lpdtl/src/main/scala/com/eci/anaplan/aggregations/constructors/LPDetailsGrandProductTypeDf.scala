package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.LPDetailsSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class LPDetailsGrandProductTypeDf @Inject()(val sparkSession: SparkSession, s3SourceService: LPDetailsSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.GrandProductTypeDf
      .select(
        $"`cost_type_id`".as("cost_type_id"),
        $"`grant_product_type`".as("grant_product_type")
      )
  }
}