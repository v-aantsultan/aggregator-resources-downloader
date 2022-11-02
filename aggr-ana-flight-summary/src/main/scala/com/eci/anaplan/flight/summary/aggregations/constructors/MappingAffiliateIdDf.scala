package com.eci.anaplan.flight.summary.aggregations.constructors

import com.eci.anaplan.flight.summary.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class MappingAffiliateIdDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
      s3SourceService.mappingAffiliateIdDf
        .select(
            $"`affiliate_id`".as("affiliate_id"),
            $"`mapping_affiliate_id`".as("mapping_affiliate_id")
        )
  }
}