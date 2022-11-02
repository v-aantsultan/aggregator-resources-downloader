package com.eci.anaplan.flight.details.aggregations.constructors

import com.eci.anaplan.flight.details.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class MappingAirlineIdDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.mappingAirlineIdDf
      .select(
        $"`airline_id`".as("airline_id"),
        $"`mapping_airline_id`".as("mapping_airline_id")
      )
  }
}