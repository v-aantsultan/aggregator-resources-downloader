package com.eci.anaplan.fa.details.aggregations.joiners

import com.eci.anaplan.fa.details.aggregations.constructors.FlightAncillariesDetailsDf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightAncillariesDetails @Inject()(spark: SparkSession, FlightAncillariesDetailsDf: FlightAncillariesDetailsDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val FlightAncillariesDetailsFinal = FlightAncillariesDetailsDf.get
      .withColumn("premium_temp",
        when($"discount_premium_idr" >= 0,$"discount_premium_idr")
          .otherwise(0)
      )

      .groupBy($"report_date", $"customer", $"business_model", $"business_partner", $"product_category", $"payment_channel")
      .agg(
        coalesce(countDistinct($"booking_id"),lit(0)).cast(IntegerType).as("no_of_transactions"),
        coalesce(sum($"coupon_code_result"),lit(0)).cast(IntegerType).as("no_of_coupon"),
        coalesce(sum($"quantity"),lit(0)).cast(IntegerType).as("transaction_volume"),
        coalesce(sum(when($"business_model" === "CONSIGNMENT",$"total_fare__contract_currency__idr")
            .otherwise($"total_fare__contract_currency__idr" + $"premium_temp")),
          lit(0)).cast(DecimalType(18,4)).as("gmv"),
        coalesce(sum($"commission_to_affiliate_idr" * -1),lit(0)).cast(DecimalType(18,4)).as("commission_to_affiliate"),
        coalesce(sum($"commission_idr" + $"total_fare___nta_idr"),lit(0)).cast(DecimalType(18,4)).as("commission"),
        coalesce(sum(
          when($"discount_premium_idr" < 0,$"discount_premium_idr" + $"discount_wht_idr")
        ),lit(0)).cast(DecimalType(18,4)).as("discount"),
        coalesce(sum($"premium_temp"),lit(0)).cast(DecimalType(18,4)).as("premium"),
        coalesce(sum($"unique_code_idr"),lit(0)).cast(DecimalType(18,4)).as("unique_code"),
        coalesce(sum($"coupon_value_idr"),lit(0)).cast(DecimalType(18,4)).as("coupon"),
        coalesce(sum($"point_redemption_idr"),lit(0)).cast(DecimalType(18,4)).as("point_redemption"),
        coalesce(sum($"mdr_charges_idr" * -1),lit(0)).cast(DecimalType(18,4)).as("mdr_charges")
      )
      .select(
        $"*"
      )
    FlightAncillariesDetailsFinal
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
