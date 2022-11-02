package com.eci.anaplan.flight.reschedule.aggregations.constructors

import com.eci.anaplan.flight.reschedule.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightRescheduleDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.FlRescheduleDf
      .select(
        $"reschedule_id",
        $"product_type",
        $"currency",
        $"refund_fee",
        $"reschedule_fee",
        $"refund_fee",
        $"reschedule_fee",
        $"old_booking_id",
        $"old_booking_issued_date",
        $"old_fulfillment_id",
        $"old_pnr",
        $"old_airline_id",
        $"old_route",
        $"new_booking_id",
        $"new_booking_issued_date",
        $"affiliate_id",
        $"corporate_id",
        $"payment_scope",
        $"business_model",
        $"report_date"
      )
  }
}