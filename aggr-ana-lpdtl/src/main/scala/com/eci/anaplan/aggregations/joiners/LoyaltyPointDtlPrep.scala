package com.eci.anaplan.aggregations.joiners

import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class LoyaltyPointDtlPrep @Inject()(spark: SparkSession, LoyaltyPointIDR: LPDetailsIDR) {

import spark.implicits._

def get: DataFrame = {
  LoyaltyPointIDR.get
    .filter(
      ($"mapping_transaction_category" === "Grant" && !$"mapping_grant_product_type".isin("Selling Points","Employee Benefits Points"))
        || ($"mapping_transaction_category" === "Redeem" && !$"mapping_underlying_product".isin("LP")))
    .select(
      $"posting_date".as("report_date"),
      $"mapping_transaction_category".as("category"),
      $"customer",
      when($"mapping_transaction_category" === "Grant", $"mapping_grant_product_type")
        .otherwise($"mapping_underlying_product").as("product_category"),
      $"original_transaction_id".as("point_transaction"),
      when($"mapping_transaction_category" === "Redeem", $"point_amount_in_transaction_currency_idr" * -1)
        .otherwise($"point_amount_in_transaction_currency_idr").as("point_amount")
    )
  }
}
