package com.eci.anaplan.bs.details.aggregations.constructors

import com.eci.anaplan.bs.details.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class MappingBusinessPartnerDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.BusinessPartnerDf
      .select(
        $"`vendor_name__sales_report_`".as("vendor_name__sales_report_"),
        $"`business_partner_mapping`".as("business_partner_mapping")
      )
  }
}
