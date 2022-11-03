package com.eci.anaplan.crn.nrd.aggr.aggregations.joiners

import com.eci.anaplan.crn.nrd.aggr.aggregations.constructors.CarRentalNrdDf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import javax.inject.{Inject, Singleton}

@Singleton
class CarRentalNrdAggr @Inject()(spark: SparkSession,
                                 carRentalNrdDf: CarRentalNrdDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    carRentalNrdDf.get.groupBy(
      $"report_date", $"customer", $"business_model", $"business_partner", $"product_category"
    ).agg(
      coalesce(countDistinct($"booking_id_fix"), lit(0)).cast(IntegerType).as("no_of_transactions"),
      coalesce(sum($"coupon_code_result"),lit(0)).cast(IntegerType).as("no_of_coupon"),
      coalesce(sum($"quantity"), lit(0)).cast(IntegerType).as("transaction_volume"),
      coalesce(sum($"publish_rate_contract_currency_idr"), lit(0)).cast(DecimalType(18,4)).as("gmv"),
      coalesce(
        when(lower($"business_model") === "consignment", 0)
        .otherwise(
          sum($"publish_rate_contract_currency_idr")
        ), lit(0)
      ).cast(DecimalType(18,4)).as("gross_sales"),
      coalesce(
        when(lower($"business_model") === "consignment", sum($"gross_commission_idr"))
          .otherwise(0),
        lit(0)
      ).cast(DecimalType(18,4)).as("commission"),
      coalesce(sum(
        when($"discount_premium_idr" < 0,$"discount_premium_idr" + $"discount_wht_idr")
      ),lit(0)).cast(DecimalType(18,4)).as("discount"),
      coalesce(sum(
        when($"discount_premium_idr" >= 0,$"discount_premium_idr")
      ),lit(0)).cast(DecimalType(18,4)).as("premium"),
      coalesce(sum($"unique_code_idr"),lit(0)).cast(DecimalType(18,4)).as("unique_code"),
      coalesce(sum($"coupon_value_idr"),lit(0)).cast(DecimalType(18,4)).as("coupon"),
      coalesce(
        when(
          lower($"business_model") === "consignment", lit(0)
        ).otherwise(sum($"nta_idr" * -1))
      ).cast(DecimalType(18,4)).as("nta"),
      coalesce(sum($"transaction_fee_idr"),lit(0)).cast(DecimalType(18,4)).as("transaction_fee"),
      coalesce(sum($"vat_out_idr" * -1),lit(0)).cast(DecimalType(18,4)).as("vat_out"),
      coalesce(sum($"point_redemption_idr"),lit(0)).cast(DecimalType(18,4)).as("point_redemption")
    ).select($"*")
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}