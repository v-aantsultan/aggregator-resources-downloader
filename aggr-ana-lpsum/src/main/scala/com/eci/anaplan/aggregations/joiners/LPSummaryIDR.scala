package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import org.apache.spark.sql.functions.{substring, to_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class LPSummaryIDR @Inject()(spark: SparkSession, LPMutationDf: LPSummaryDf, ExchangeRateDf: LPSummaryRateDf,
                             GrandProductTypeDf: LPSummaryGrandProductTypeDf, TransactionCategoryDf: LPSummaryTransactionCategoryDf,
                             UnderlyingProductDf: LPSummaryUnderlyingProductDf) {

import spark.implicits._

def get: DataFrame = {
    LPMutationDf.get.as("lp_mutation")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
          $"lp_mutation.transaction_currency" === $"exchange_rate_idr.from_currency"
            && to_timestamp($"lp_mutation.posting_date") === $"exchange_rate_idr.conversion_date"
          , "left")
      .join(GrandProductTypeDf.get.as("mapping_grant_product_type"),
          $"lp_mutation.cost_type_id" === $"mapping_grant_product_type.cost_type_id"
          , "left")
      .join(TransactionCategoryDf.get.as("mapping_transaction_category"),
          $"lp_mutation.transaction_type" === $"mapping_transaction_category.transaction_type"
          , "left")
      .join(UnderlyingProductDf.get.as("mapping_underlying_product"),
          $"lp_mutation.booking_product_type" === $"mapping_underlying_product.fs_product_type"
          , "left")

      .select(
          $"lp_mutation.*",
          $"mapping_underlying_product.underlying_product".as("mapping_underlying_product"),
          $"mapping_transaction_category.transaction_category".as("mapping_transaction_category"),
          $"mapping_grant_product_type.grant_product_type".as("mapping_grant_product_type"),
          substring($"lp_mutation.transaction_currency",1,2).as("customer"),
          when($"lp_mutation.transaction_currency" === "IDR",$"lp_mutation.point_amount")
            .otherwise($"lp_mutation.point_amount" * $"exchange_rate_idr.conversion_rate")
            .as("point_amount_idr"),
          when($"lp_mutation.transaction_currency" === "IDR",$"lp_mutation.point_amount_in_transaction_currency")
            .otherwise($"lp_mutation.point_amount_in_transaction_currency" * $"exchange_rate_idr.conversion_rate")
            .as("point_amount_in_transaction_currency_idr"),
          when($"lp_mutation.transaction_currency" === "IDR",$"lp_mutation.earning_amount")
            .otherwise($"lp_mutation.earning_amount" * $"exchange_rate_idr.conversion_rate")
            .as("earning_amount_idr"),
          when($"lp_mutation.transaction_currency" === "IDR",$"lp_mutation.granted_point_wht_in_transaction_currency")
            .otherwise($"lp_mutation.granted_point_wht_in_transaction_currency" * $"exchange_rate_idr.conversion_rate")
            .as("granted_point_wht_in_transaction_currency_idr")
      )
  }
}
