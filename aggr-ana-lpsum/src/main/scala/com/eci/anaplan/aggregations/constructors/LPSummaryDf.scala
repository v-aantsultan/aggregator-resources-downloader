package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.LPSummarySource
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class LPSummaryDf @Inject()(val sparkSession: SparkSession, s3SourceService: LPSummarySource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.LPMutationDf
        .select(
          $"`posting_date`".as("posting_date"),
          $"`original_transaction_id`".as("original_transaction_id"),
          $"`booking_product_type`".as("booking_product_type"),
          $"`transaction_type`".as("transaction_type"),
          $"`cost_type_id`".as("cost_type_id"),
          $"`transaction_currency`".as("transaction_currency"),
          $"`point_amount`".as("point_amount"),
          $"`point_conversion_rate`".as("point_conversion_rate"),
          $"`point_amount_in_transaction_currency`".as("point_amount_in_transaction_currency"),
          $"`selling_rate`".as("selling_rate"),
          $"`earning_amount`".as("earning_amount"),
          $"`granted_point_wht_in_transaction_currency`".as("granted_point_wht_in_transaction_currency")
        )
  }
}
