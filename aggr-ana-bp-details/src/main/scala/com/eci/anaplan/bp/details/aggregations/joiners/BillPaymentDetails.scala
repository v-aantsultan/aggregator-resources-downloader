package com.eci.anaplan.bp.details.aggregations.joiners

import com.eci.anaplan.bp.details.aggregations.constructors.BPDetailsDf
import org.apache.spark.sql.functions.{coalesce, count, countDistinct, lit, substring, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class BillPaymentDetails @Inject()(spark: SparkSession,
                                   BPDetailsDf: BPDetailsDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val BillPaymentDetailsFinal = BPDetailsDf.get
      .groupBy($"report_date", $"customer", $"business_model", $"business_partner", $"product_category", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"),lit(0)).as("no_of_coupon"),
        coalesce(count($"booking_id"),lit(0)).as("transaction_volume"),
        coalesce(sum($"published_rate_in_selling_currency"),lit(0)).as("gmv"),
        coalesce(sum($"commission_revenue"),lit(0)).as("commission"),
        coalesce(sum($"transaction_fee"),lit(0)).as("transaction_fee"),
        coalesce(sum(
          when($"discount_or_premium" < 0,$"discount_or_premium" + $"discount_wht")
        ),lit(0)).as("discount"),
        coalesce(sum(
          when($"discount_or_premium" >= 0,$"discount_or_premium")
        ),lit(0)).as("premium"),
        coalesce(sum($"unique_code"),lit(0)).as("unique_code"),
        coalesce(sum($"total_coupon_value"),lit(0)).as("coupon"),
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
    BillPaymentDetailsFinal
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
