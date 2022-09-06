package com.eci.anaplan.train.details.aggregations.joiners

import com.eci.anaplan.train.details.aggregations.constructors.GlobalTrainDetailsDf
import org.apache.spark.sql.functions.{coalesce, countDistinct, lit, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class GlobalTrainDetails @Inject()(spark: SparkSession,
                                   GlobalTrainDetailsDf: GlobalTrainDetailsDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val GlobalTrainDetailsFinal = GlobalTrainDetailsDf.get
      .groupBy($"report_date", $"customer", $"business_model", $"business_partner", $"product_category", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"),lit(0)).as("no_of_coupon"),
        coalesce(sum($"pax_quantity"),lit(0)).as("transaction_volume"),
        coalesce(sum($"gmv_result"),lit(0)).as("gmv"),
        coalesce(sum($"gross_revenue_result"),lit(0)).as("gross_revenue"),
        coalesce(sum($"commission_result"),lit(0)).as("commission"),
        coalesce(sum(
          when($"discount_or_premium_idr" < 0,$"discount_or_premium_idr" + $"wht_discount_idr")
        ),lit(0)).as("discount"),
        coalesce(sum(
          when($"discount_or_premium_idr" >= 0,$"discount_or_premium_idr")
        ),lit(0)).as("premium"),
        coalesce(sum($"unique_code_idr"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon_amount_idr"),lit(0)).as("coupon"),
        coalesce(sum($"nta_result"),lit(0)).as("nta"),
        coalesce(sum($"transaction_fee_idr"),lit(0)).as("transaction_fee"),
        coalesce(sum($"vat_out_idr" * -1),lit(0)).as("vat_out"),
        coalesce(sum($"point_redemption_idr"),lit(0)).as("point_redemption"),
        coalesce(sum($"mdr_charges_idr" * -1),lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )
    GlobalTrainDetailsFinal
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
