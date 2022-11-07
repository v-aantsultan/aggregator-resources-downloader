package com.eci.anaplan.hs.details.aggregations.joiners

import com.eci.anaplan.hs.details.aggregations.constructors.HSDetailsDf
import org.apache.spark.sql.functions.{coalesce, countDistinct, lit, sum}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class HealthServiceDetails @Inject()(spark: SparkSession, HSDetailsDf: HSDetailsDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val HealthServiceDetailsFinal = HSDetailsDf.get
      .groupBy(
        $"report_date",
        $"customer",
        $"business_model",
        $"business_partner",
        $"product",
        $"product_category",
        coalesce($"payment_channel",lit("None")).as("payment_channel")
      )
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).cast(IntegerType).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"),lit(0)).cast(IntegerType).as("no_of_coupon"),
        coalesce(sum($"number_of_tickets"),lit(0)).cast(IntegerType).as("transaction_volume"),
        coalesce(sum($"gmv"),lit(0)).cast(DecimalType(18,4)).as("gmv"),
        coalesce(sum($"actual_gross_commission_contract_currency"),lit(0)).cast(DecimalType(18,4)).as("commission"),
        coalesce(sum($"discount"),lit(0)).cast(DecimalType(18,4)).as("discount"),
        coalesce(sum($"premium"),lit(0)).cast(DecimalType(18,4)).as("premium"),
        coalesce(sum($"unique_code"),lit(0)).cast(DecimalType(18,4)).as("unique_code"),
        coalesce(sum($"coupon_value"),lit(0)).cast(DecimalType(18,4)).as("coupon"),
        coalesce(sum($"gross_sales"),lit(0)).cast(DecimalType(18,4)).as("gross_sales"),
        coalesce(sum($"nta"),lit(0)).cast(DecimalType(18,4)).as("nta"),
        coalesce(sum($"transaction_fee"),lit(0)).cast(DecimalType(18,4)).as("transaction_fee"),
        coalesce(sum($"point_redemption"),lit(0)).cast(DecimalType(18,4)).as("point_redemption"),
        coalesce(sum($"vat_out"),lit(0)).cast(DecimalType(18,4)).as("vat_out"),
        coalesce(sum($"mdr_charges"),lit(0)).cast(DecimalType(18,4)).as("mdr_charges")
      )
      .select(
        $"*"
      )

    HealthServiceDetailsFinal

  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
