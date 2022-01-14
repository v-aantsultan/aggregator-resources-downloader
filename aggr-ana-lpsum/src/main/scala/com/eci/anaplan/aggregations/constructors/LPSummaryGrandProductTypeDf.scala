package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.LPSummarySource
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class LPSummaryGrandProductTypeDf @Inject()(val sparkSession: SparkSession, s3SourceService: LPSummarySource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.GrandProductTypeDf
      .select(
        $"`cost_type_id`".as("cost_type_id"),
        $"`grant_product_type`".as("grant_product_type")
      )
  }
}