package com.eci.anaplan.fa.aggregations.constructors

import com.eci.anaplan.fa.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class MappingFulfillmentIDDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.FulfillmentIDDf
      .select(
        $"`no`".as("no"),
        $"`fulfillment_id`".as("fulfillment_id"),
        $"`wholesaler`".as("wholesaler")
      )
  }
}
