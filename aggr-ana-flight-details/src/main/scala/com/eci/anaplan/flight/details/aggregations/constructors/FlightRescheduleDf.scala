package com.eci.anaplan.flight.details.aggregations.constructors

import com.eci.anaplan.flight.reschedule.aggregations.joiners.FlightRescheduleFinalJoiner
import org.apache.spark.sql.functions.{coalesce, lit, regexp_replace, split, sum, to_date, trim, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightRescheduleDf @Inject()(val sparkSession: SparkSession, FlightRescheduleFinalJoiner: FlightRescheduleFinalJoiner) {

  import sparkSession.implicits._

  def get: DataFrame = {
    FlightRescheduleFinalJoiner.get
      .withColumn("payment_channel_clear",
        regexp_replace(
          regexp_replace($"payment_scope",
            "adjustment,",""),
          "adjustment_refund,","")
      )
      .select(
        to_date($"new_booking_issued_date".cast(StringType), "dd/MM/yyyy").as("report_date"),
        when($"corporate_id".isNull || $"affiliate_id" === "N/A", lit("ID"))
          .when($"affiliate_id".isNull || $"affiliate_id" === "N/A", lit("ID"))
          .otherwise(lit("Affiliates Others"))
          .as("customer"),
        $"business_model",
        when($"wholesaler" === "N/A", lit("Direct Airlines"))
          .otherwise($"wholesaler")
          .as("business_partner"),
        lit("FL").as("product"),
        lit("Domestic").as("product_category"),
        coalesce(trim(split($"payment_channel_clear",",")(0)), lit("None")).as("payment_channel"),
        $"old_airline_id",
        lit(0).as("no_of_transactions"),
        lit(0).as("no_of_coupon"),
        lit(0).as("transaction_volume"),
        lit(0).as("no_of_point_redemption"),
        lit(0).as("gmv"),
        lit(0).as("commission"),
        lit(0).as("discount"),
        lit(0).as("premium"),
        lit(0).as("unique_code"),
        lit(0).as("coupon"),
        lit(0).as("nta"),
        lit(0).as("transaction_fee"),
        lit(0).as("point_redemption"),
        lit(0).as("handling_fee"),
        lit(0).as("commission_to_affiliate"),
        lit(0).as("incentive"),
        lit(0).as("rebook_cost"),
        lit(0).as("basic_reshcedule_fee"),
        $"reschedule_fee_idr".as("premium_reschedule_fee"),
        $"refund_fee_idr".as("refund_fee"),
        ($"reschedule_fee_idr" + $"refund_fee_idr").as("total_reschedule_fee"),
        lit(0).as("mdr_charges")
      )
  }
}
