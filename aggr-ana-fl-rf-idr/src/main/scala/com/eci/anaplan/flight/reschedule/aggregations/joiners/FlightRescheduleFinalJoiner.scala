package com.eci.anaplan.flight.reschedule.aggregations.joiners

import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightRescheduleFinalJoiner @Inject()(spark: SparkSession,
                                            flightRescheduleFlattenerJoiner: FlightRescheduleFlattenerJoiner) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._
    flightRescheduleFlattenerJoiner.get
      .select(
        $"reschedule_id",
        $"product_type",
        $"currency",
        $"refund_fee",
        $"reschedule_fee",
        $"refund_fee_idr",
        $"reschedule_fee_idr",
        $"old_booking_id",
        $"old_booking_issued_date",
        $"old_fulfillment_id",
        $"wholesaler",
        $"old_pnr",
        $"old_airline_id",
        $"old_route",
        $"new_booking_id",
        $"new_booking_issued_date",
        $"affiliate_id",
        $"corporate_id",
        $"payment_scope",
        $"business_model",
        to_date($"report_date".cast(StringType), "yyyyMMdd").as("report_date")
      )
  }

  def get: DataFrame =
    joinDataFrames
}