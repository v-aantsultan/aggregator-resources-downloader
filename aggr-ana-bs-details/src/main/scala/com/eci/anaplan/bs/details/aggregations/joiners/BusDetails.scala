package com.eci.anaplan.bs.details.aggregations.joiners

import com.eci.anaplan.bs.details.aggregations.constructors.BusDetailsDf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class BusDetails @Inject()(spark: SparkSession, BusDetailsDf: BusDetailsDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val BusDetailsFinal = BusDetailsDf.get
      .groupBy($"report_date",
        $"customer",
        $"business_model",
        $"business_partner",
        $"product",
        coalesce($"payment_channel",lit("None")).as("payment_channel"))
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"),lit(0)).as("no_of_coupon"),
        coalesce(sum($"number_of_seats"),lit(0)).as("transaction_volume"),
        coalesce(sum($"publish_rate_contract_currency"),lit(0)).as("gmv"),
        coalesce(sum($"commission_result"),lit(0)).as("commission"),
        coalesce(sum($"commission_expense_value" * -1),lit(0)).as("commission_to_affiliate"),
        coalesce(sum(
          when($"discount_premium" < 0,$"discount_premium" + $"wht_discount")
        ),lit(0)).as("discount"),
        coalesce(sum(
          when($"discount_premium" >= 0,$"discount_premium")
        ),lit(0)).as("premium"),
        coalesce(sum($"final_unique_code"),lit(0)).as("unique_code"),
        coalesce(sum($"final_coupon_value"),lit(0)).as("coupon"),
        coalesce(sum($"vat_out" * -1),lit(0)).as("vat_out"),
        coalesce(sum($"final_point_redemption"),lit(0)).as("point_redemption"),
        coalesce(sum($"final_mdr_charges" * -1),lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )
    BusDetailsFinal
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
