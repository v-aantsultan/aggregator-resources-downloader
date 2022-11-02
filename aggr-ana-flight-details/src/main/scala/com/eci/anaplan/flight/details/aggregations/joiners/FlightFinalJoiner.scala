package com.eci.anaplan.flight.details.aggregations.joiners

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightFinalJoiner @Inject()(spark: SparkSession,
                                  flightFlattenerJoiner: FlightFlattenerJoiner,
                                  FlightRescheduleFlattenerJoiner: FlightRescheduleFlattenerJoiner) {

  import spark.implicits._

  def get: DataFrame = {

    val FlightwithFulfillment = flightFlattenerJoiner.get
      .groupBy(
        $"report_date",$"customer",$"business_model",$"fulfillment_id",$"business_partner",$"product",
        $"product_category",$"payment_channel",$"airline_id"
      )
      .agg(
        coalesce(countDistinct($"booking_id"), lit(0)).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"), lit(0)).as("no_of_coupon"),
        coalesce(sum($"total_segments_pax__exclude_infant_"), lit(0)).as("transaction_volume"),
        coalesce(count($"is_point_redemption_idr"), lit(0)).as("no_of_point_redemption"),
        coalesce(sum($"gmv_ready"), lit(0)).as("gmv"),
        coalesce(sum($"commission_ready"), lit(0)).as("commission"),
        coalesce(sum($"discount_ready"), lit(0)).as("discount"),
        coalesce(sum($"premium_ready"), lit(0)).as("premium"),
        coalesce(sum($"unique_code_idr"), lit(0)).as("unique_code"),
        coalesce(sum($"coupon_value_idr"), lit(0)).as("coupon"),
        coalesce(sum($"total_fare__contract_currency__idr" * lit(-1)), lit(0)).as("nta"),
        coalesce(sum($"transaction_fee_idr"), lit(0)).as("transaction_fee"),
        coalesce(sum($"point_redemption_idr"), lit(0)).as("point_redemption"),
        coalesce(sum($"issuance_fee_idr" * lit(-1)), lit(0)).as("handling_fee"),
        coalesce(sum($"commission_to_affiliate_idr" * lit(-1)), lit(0)).as("commission_to_affiliate"),
        coalesce(sum($"basic_incentive_idr"), lit(0)).as("incentive"),
        coalesce(sum($"rebook_cost_idr"), lit(0)).as("rebook_cost"),
        coalesce(sum($"traveloka_reschedule_fee__contract_currency__idr"), lit(0)).as("basic_reshcedule_fee"),
        coalesce(sum($"premium_reschedule_fee_idr"), lit(0)).as("premium_reschedule_fee"),
        coalesce(sum($"refund_fee_idr"), lit(0)).as("refund_fee"),
        coalesce(sum($"total_reschedule_fee_ready")).as("total_reschedule_fee"),
        coalesce(sum($"mdr_charges_idr" * lit(-1)), lit(0)).as("mdr_charges")
      )
      .select($"*")

    val flight = FlightwithFulfillment
      .groupBy(
        $"report_date",$"customer",$"business_model",$"business_partner",$"product",$"product_category",$"payment_channel",$"airline_id"
      )
      .agg(
        sum($"no_of_transactions").as("no_of_transactions"),
        sum($"no_of_coupon").as("no_of_coupon"),
        sum($"transaction_volume").as("transaction_volume"),
        sum($"no_of_point_redemption").as("no_of_point_redemption"),
        sum($"gmv").as("gmv"),
        sum($"commission").as("commission"),
        sum($"discount").as("discount"),
        sum($"premium").as("premium"),
        sum($"unique_code").as("unique_code"),
        sum($"coupon").as("coupon"),
        sum($"nta").as("nta"),
        sum($"transaction_fee").as("transaction_fee"),
        sum($"point_redemption").as("point_redemption"),
        sum($"handling_fee").as("handling_fee"),
        sum($"commission_to_affiliate").as("commission_to_affiliate"),
        sum($"incentive").as("incentive"),
        sum($"rebook_cost").as("rebook_cost"),
        sum($"basic_reshcedule_fee").as("basic_reshcedule_fee"),
        sum($"premium_reschedule_fee").as("premium_reschedule_fee"),
        sum($"refund_fee").as("refund_fee"),
        sum($"total_reschedule_fee").as("total_reschedule_fee"),
        sum($"mdr_charges").as("mdr_charges")
      )
      .select($"*")

    val FlightReschedule = FlightRescheduleFlattenerJoiner.get
      .groupBy(
        $"report_date",$"customer",$"business_model",$"business_partner",$"product",$"product_category",$"payment_channel",$"airline_id"
      )
      .agg(
        coalesce(sum($"no_of_transactions"),lit(0)).as("no_of_transactions"),
        coalesce(sum($"no_of_coupon"),lit(0)).as("no_of_coupon"),
        coalesce(sum($"transaction_volume"),lit(0)).as("transaction_volume"),
        coalesce(sum($"no_of_point_redemption"),lit(0)).as("no_of_point_redemption"),
        coalesce(sum($"gmv"),lit(0)).as("gmv"),
        coalesce(sum($"commission"),lit(0)).as("commission"),
        coalesce(sum($"discount"),lit(0)).as("discount"),
        coalesce(sum($"premium"),lit(0)).as("premium"),
        coalesce(sum($"unique_code"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon"),lit(0)).as("coupon"),
        coalesce(sum($"nta"),lit(0)).as("nta"),
        coalesce(sum($"transaction_fee"),lit(0)).as("transaction_fee"),
        coalesce(sum($"point_redemption"),lit(0)).as("point_redemption"),
        coalesce(sum($"handling_fee"),lit(0)).as("handling_fee"),
        coalesce(sum($"commission_to_affiliate"),lit(0)).as("commission_to_affiliate"),
        coalesce(sum($"incentive"),lit(0)).as("incentive"),
        coalesce(sum($"rebook_cost"),lit(0)).as("rebook_cost"),
        coalesce(sum($"basic_reshcedule_fee"),lit(0)).as("basic_reshcedule_fee"),
        coalesce(sum($"premium_reschedule_fee"),lit(0)).as("premium_reschedule_fee"),
        coalesce(sum($"refund_fee"),lit(0)).as("refund_fee"),
        coalesce(sum($"total_reschedule_fee"),lit(0)).as("total_reschedule_fee"),
        coalesce(sum($"mdr_charges"),lit(0)).as("mdr_charges")
      )
      .select($"*")

    val combined = flight.union(FlightReschedule)

    combined
      .groupBy(
        $"report_date",$"customer",$"business_model",$"business_partner",$"product",$"product_category",$"payment_channel",$"airline_id"
      )
      .agg(
        sum($"no_of_transactions").cast(IntegerType).as("no_of_transactions"),
        sum($"no_of_coupon").cast(IntegerType).as("no_of_coupon"),
        sum($"transaction_volume").cast(IntegerType).as("transaction_volume"),
        sum($"no_of_point_redemption").cast(IntegerType).as("no_of_point_redemption"),
        sum($"gmv").cast(DecimalType(18,4)).as("gmv"),
        sum($"commission").cast(DecimalType(18,4)).as("commission"),
        sum($"discount").cast(DecimalType(18,4)).as("discount"),
        sum($"premium").cast(DecimalType(18,4)).as("premium"),
        sum($"unique_code").cast(DecimalType(18,4)).as("unique_code"),
        sum($"coupon").cast(DecimalType(18,4)).as("coupon"),
        sum($"nta").cast(DecimalType(18,4)).as("nta"),
        sum($"transaction_fee").cast(DecimalType(18,4)).as("transaction_fee"),
        sum($"point_redemption").cast(DecimalType(18,4)).as("point_redemption"),
        sum($"handling_fee").cast(DecimalType(18,4)).as("handling_fee"),
        sum($"commission_to_affiliate").cast(DecimalType(18,4)).as("commission_to_affiliate"),
        sum($"incentive").cast(DecimalType(18,4)).as("incentive"),
        sum($"rebook_cost").cast(DecimalType(18,4)).as("rebook_cost"),
        sum($"basic_reshcedule_fee").cast(DecimalType(18,4)).as("basic_reshcedule_fee"),
        sum($"premium_reschedule_fee").cast(DecimalType(18,4)).as("premium_reschedule_fee"),
        sum($"refund_fee").cast(DecimalType(18,4)).as("refund_fee"),
        sum($"total_reschedule_fee").cast(DecimalType(18,4)).as("total_reschedule_fee"),
        sum($"mdr_charges").cast(DecimalType(18,4)).as("mdr_charges")
      )
      .select($"*")

  }
}