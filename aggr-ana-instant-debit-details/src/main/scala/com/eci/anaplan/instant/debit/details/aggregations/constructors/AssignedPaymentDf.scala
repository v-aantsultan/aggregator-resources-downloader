package com.eci.anaplan.instant.debit.details.aggregations.constructors

import com.eci.anaplan.instant.debit.details.services.S3SourceService
import org.apache.spark.sql.functions.{expr, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class AssignedPaymentDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.AssignedPaymentSrc
      .select(
        $"payment_id",
        $"payment_request_id",
        $"payment_scope",
        $"incoming_fund_entity",
        $"selling_entity",
        $"booking_id",
        $"product_type",
        $"payment_currency",
        $"payment_amount",
        $"invoice_amount",
        $"payment_time",
        $"payment_assignment_time",
        $"issued_time",
        $"b2b_type",
        $"b2b_partners_id",
        to_date($"issued_time" + expr("INTERVAL 7 HOURS")).as("issued_time_formatted")
      )
  }
}