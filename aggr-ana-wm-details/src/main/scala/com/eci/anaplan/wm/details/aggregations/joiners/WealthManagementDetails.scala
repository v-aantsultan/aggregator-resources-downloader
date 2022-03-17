package com.eci.anaplan.wm.details.aggregations.joiners

import com.eci.anaplan.wm.details.aggregations.constructors.WMDetailsDf
import org.apache.spark.sql.functions.{coalesce, count, countDistinct, lit, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class WealthManagementDetails @Inject()(spark: SparkSession,
                                        WMDetailsDf: WMDetailsDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val WealthManagementDetailsFinal = WMDetailsDf.get
      .groupBy($"report_date", $"customer", $"business_model", $"business_partner", $"product", $"product_category", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).as("no_of_transactions"),
        coalesce(count($"coupon_value_result"),lit(0)).as("no_of_coupon"),
        coalesce(count($"booking_id"),lit(0)).as("transaction_volume"),
        coalesce(sum($"topup_amount"),lit(0)).as("gmv"),
        coalesce(sum($"commission_revenue"),lit(0)).as("commission"),
        coalesce(sum($"unique_code"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon_value"),lit(0)).as("coupon"),
        coalesce(sum(
          when($"report_date".lt(lit("2022-04-01")), $"commission_revenue" * (-0.1))
            .otherwise($"commission_revenue" * (-.11))),lit(0))
          .as("vat_out"),
        coalesce(sum($"point_redemption"),lit(0)).as("point_redemption"),
        coalesce(sum($"mdr_amount_prorate_idr" * -1),lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )
    WealthManagementDetailsFinal
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
