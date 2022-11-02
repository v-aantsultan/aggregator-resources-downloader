package com.eci.anaplan.flight.idr.aggregations.constructors

import com.eci.anaplan.flight.idr.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlattenerDimCountryDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.mappingDimCountryDf
      .select(
        $"`country_id`".as("country_id"),
        $"`country_name`".as("country_name")
      )
  }
}