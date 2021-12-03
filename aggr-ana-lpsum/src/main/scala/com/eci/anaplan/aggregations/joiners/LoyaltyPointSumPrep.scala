package com.eci.anaplan.aggregations.joiners

import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class LoyaltyPointSumPrep @Inject()(spark: SparkSession, LoyaltyPointIDR: LPSummaryIDR) {

import spark.implicits._

def get: DataFrame = {
  LoyaltyPointIDR.get
    .select(
      $"posting_date",
      $"posting_date".as("date"),
      $"customer",
      when($"mapping_transaction_category" === "Grant" && $"mapping_grant_product_type" === "Selling Points",
        $"original_transaction_id")
        .otherwise(null).as("selling_point_transactions"),
      when($"mapping_transaction_category" === "Grant" && $"mapping_grant_product_type" === "Selling Points",
        $"point_amount_in_transaction_currency_idr")
        .otherwise(0).as("selling_point_amount"),
      when($"mapping_transaction_category" === "Grant" && $"mapping_grant_product_type" === "Employee Reward Points",
        $"original_transaction_id")
        .otherwise(null).as("employee_benefit_points_transactions"),
      when($"mapping_transaction_category" === "Grant" && $"mapping_grant_product_type" === "Employee Reward Points",
        $"point_amount_in_transaction_currency_idr")
        .otherwise(0).as("employee_benefit_points_amount"),
      when($"mapping_transaction_category" === "Redeem" && $"mapping_underlying_product" === "LP",
        $"original_transaction_id")
        .otherwise(null).as("point_catalogue_transactions"),
      when($"mapping_transaction_category" === "Redeem" && $"mapping_underlying_product" === "LP",
        $"point_amount_in_transaction_currency_idr" * -1)
        .otherwise(0).as("point_catalogue_amount"),
      when($"mapping_transaction_category".isin("Redeem", "Expiration") && $"cost_type_id" =!= "LP_ENTITY_NEW",
        $"point_amount_idr" * ($"point_conversion_rate" - $"selling_rate"))
        .otherwise(0).as("discount"),
      when($"mapping_transaction_category" === "Expiration",
        $"point_amount_in_transaction_currency_idr")
        .otherwise(0).as("expired")
    )
  }
}
