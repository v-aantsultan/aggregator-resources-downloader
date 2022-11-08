package com.eci.anaplan.hs_nrd.details.aggregations.joiners

import com.eci.anaplan.hs_nrd.details.aggregations.constructors.HealthServicesNrdDf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class HealthServiceDetails @Inject()(spark: SparkSession, healthServicesNrdDf: HealthServicesNrdDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val BusinessModelisConsigment = upper($"business_model") === lit("CONSIGNMENT")

    val HSNRDTransform = healthServicesNrdDf.get
      .withColumn("coupon_code_split",
        when(size(split($"coupon_code", ",")) === 0, 1)
          .otherwise(size(split($"coupon_code", ",")))
      )

      .select(
        to_date($"non_refundable_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        split($"locale","_")(1).as("customer"),
        $"business_model".as("business_model"),
        $"fulfillment_id".as("business_partner"),
        lit("HEALTHCARE").as("product"),
        upper($"category").as("product_category"),
        $"booking_id".as("no_of_transaction"),
        when($"coupon_code" === "N/A" || $"coupon_code".isNull || $"coupon_code" === "", 0)
          .otherwise($"coupon_code_split")
          .as("no_of_coupon"),
        $"number_of_tickets".as("transaction_volume"),
        when(BusinessModelisConsigment, $"recommended_price_contract_currency")
          .otherwise($"published_rate_contract_currency")
          .as("gmv"),
        when(BusinessModelisConsigment, $"actual_gross_commission_contract_currency")
          .otherwise(lit(0))
          .as("commission"),
        when($"discount_premium_incoming_fund_currency".leq(lit(0)),
          $"discount_premium_incoming_fund_currency" + $"discount_tax_expense")
          .otherwise(lit(0))
          .as("discount"),
        when($"discount_premium_incoming_fund_currency".geq(lit(0)),
          $"discount_premium_incoming_fund_currency")
          .otherwise(lit(0))
          .as("premium"),
        $"unique_code",
        $"coupon_value".as("coupon"),
        when(BusinessModelisConsigment, lit(0))
          .otherwise($"published_rate_contract_currency")
          .as("gross_sales"),
        when(BusinessModelisConsigment, lit(0))
          .otherwise($"nta_contract_currency")
          .as("nta"),
        $"transaction_fee",
        $"point_redemption",
        ($"vat_out_contract_currency" * -1).as("vat_out")
      )

    val HSNRDGroupFInal = HSNRDTransform
      .groupBy(
        $"report_date", $"customer", $"business_model", $"business_partner", $"product", $"product_category"
      )
      .agg(
        coalesce(countDistinct($"no_of_transaction"), lit(0)).cast(IntegerType).as("no_of_transactions"),
        coalesce(sum($"no_of_coupon"), lit(0)).cast(IntegerType).as("no_of_coupon"),
        coalesce(sum($"transaction_volume"), lit(0)).cast(IntegerType).as("transaction_volume"),
        coalesce(sum($"gmv"), lit(0)).cast(DecimalType(18,4)).as("gmv"),
        coalesce(sum($"commission"), lit(0)).cast(DecimalType(18,4)).as("commission"),
        coalesce(sum($"discount"), lit(0)).cast(DecimalType(18,4)).as("discount"),
        coalesce(sum($"premium"), lit(0)).cast(DecimalType(18,4)).as("premium"),
        coalesce(sum($"unique_code"), lit(0)).cast(DecimalType(18,4)).as("unique_code"),
        coalesce(sum($"coupon"), lit(0)).cast(DecimalType(18,4)).as("coupon"),
        coalesce(sum($"gross_sales"), lit(0)).cast(DecimalType(18,4)).as("gross_sales"),
        coalesce(sum($"nta"), lit(0)).cast(DecimalType(18,4)).as("nta"),
        coalesce(sum($"transaction_fee"), lit(0)).cast(DecimalType(18,4)).as("transaction_fee"),
        coalesce(sum($"point_redemption"), lit(0)).cast(DecimalType(18,4)).as("point_redemption"),
        coalesce(sum($"vat_out"), lit(0)).cast(DecimalType(18,4)).as("vat_out")
      )
      .select(
        $"*"
      )

    HSNRDGroupFInal

  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}