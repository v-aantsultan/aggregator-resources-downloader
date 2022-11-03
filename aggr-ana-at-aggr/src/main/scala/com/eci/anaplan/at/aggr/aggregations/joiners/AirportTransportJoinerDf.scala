package com.eci.anaplan.at.aggr.aggregations.joiners

import com.eci.anaplan.at.aggr.aggregations.constructors.AirportTransportDf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import javax.inject.{Inject, Singleton}

@Singleton
class AirportTransportJoinerDf @Inject()(spark: SparkSession,
                                         airportTransportDf: AirportTransportDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    airportTransportDf.get.groupBy(
      $"report_date", $"customer", $"business_model", $"business_partner", $"product_category", $"payment_channel"
    ).agg(
      coalesce(countDistinct($"no_of_tr"), lit(0)).cast(IntegerType).as("no_of_transactions"),
      coalesce(sum($"coupon_code_result"),lit(0)).cast(IntegerType).as("no_of_coupon"),
      coalesce(sum($"quantity"), lit(0)).cast(IntegerType).as("transaction_volume"),
      coalesce(sum($"total_published_rate_contract_currency_idr"), lit(0)).cast(DecimalType(18,4)).as("gmv"),
      coalesce(sum($"gross_commission_idr"), lit(0)).cast(DecimalType(18,4)).as("commission"),
      coalesce(
        sum(when($"discount_premium_idr" < 0,$"discount_premium_idr" + $"discount_wht_idr")),
        lit(0)).cast(DecimalType(18,4)).as("discount"),
      coalesce(
        sum(when($"discount_premium_idr" >= 0,$"discount_premium_idr")),
        lit(0)).cast(DecimalType(18,4)).as("premium"),
      coalesce(sum($"unique_code_idr"),lit(0)).cast(DecimalType(18,4)).as("unique_code"),
      coalesce(sum($"coupon_value_idr"),lit(0)).cast(DecimalType(18,4)).as("coupon"),
      coalesce(sum($"convenience_fee_idr"),lit(0)).cast(DecimalType(18,4)).as("transaction_fee"),
      coalesce(sum($"vat_out_idr" * -1),lit(0)).cast(DecimalType(18,4)).as("vat_out"),
      coalesce(sum($"point_redemption_idr"),lit(0)).cast(DecimalType(18,4)).as("point_redemption"),
      coalesce(sum($"rebook_cost_idr"),lit(0)).cast(DecimalType(18,4)).as("rebook_cost"),
      coalesce(sum($"mdr_charges_idr" * -1),lit(0)).cast(DecimalType(18,4)).as("mdr_charges")
    )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}