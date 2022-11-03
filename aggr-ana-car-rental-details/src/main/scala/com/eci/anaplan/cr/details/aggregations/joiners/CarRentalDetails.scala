package com.eci.anaplan.cr.details.aggregations.joiners

import com.eci.anaplan.cr.details.aggregations.constructors.CRDetailsDf
import org.apache.spark.sql.functions.{coalesce, countDistinct, lit, sum, upper, when}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class CarRentalDetails @Inject()(spark: SparkSession,
                                 CRDetailsDf: CRDetailsDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val CarRentalDetailsFinal = CRDetailsDf.get
      .groupBy($"report_date", $"customer", $"business_model", $"business_partner", $"product_category", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).cast(IntegerType).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"),lit(0)).cast(IntegerType).as("no_of_coupon"),
        coalesce(sum($"quantity"),lit(0)).cast(IntegerType).as("transaction_volume"),
        coalesce(sum($"publish_rate_contract_currency_idr"),lit(0)).cast(DecimalType(18,4)).as("gmv"),
        coalesce(sum(when(upper($"business_model") === "CONSIGNMENT", 0)
          .otherwise($"publish_rate_contract_currency_idr")),lit(0))
          .cast(DecimalType(18,4)).as("gross_sales"),
        coalesce(sum(when(upper($"business_model") === "CONSIGNMENT", $"gross_commission_idr")
          .otherwise(0)),lit(0))
          .cast(DecimalType(18,4)).as("commission"),
        coalesce(sum(
          when($"discount_premium_idr" < 0,$"discount_premium_idr" + $"discount_wht_idr")
        ),lit(0)).cast(DecimalType(18,4)).as("discount"),
        coalesce(sum(
          when($"discount_premium_idr" >= 0,$"discount_premium_idr")
        ),lit(0)).cast(DecimalType(18,4)).as("premium"),
        coalesce(sum($"unique_code_idr"),lit(0)).cast(DecimalType(18,4)).as("unique_code"),
        coalesce(sum($"coupon_value_idr"),lit(0)).cast(DecimalType(18,4)).as("coupon"),
        coalesce(sum(when(upper($"business_model") === "CONSIGNMENT", 0)
          .otherwise($"nta_idr")),lit(0))
          .cast(DecimalType(18,4)).as("nta"),
        coalesce(sum($"transaction_fee_idr"),lit(0)).cast(DecimalType(18,4)).as("transaction_fee"),
        coalesce(sum($"vat_out_idr" * -1),lit(0)).cast(DecimalType(18,4)).as("vat_out"),
        coalesce(sum($"point_redemption_idr"),lit(0)).cast(DecimalType(18,4)).as("point_redemption"),
        coalesce(sum($"mdr_charges_idr" * -1),lit(0)).cast(DecimalType(18,4)).as("mdr_charges")
      )
      .select(
        $"*"
      )
    CarRentalDetailsFinal
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
