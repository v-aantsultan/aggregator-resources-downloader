package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import com.eci.anaplan.services.LPMutationStatusManager
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{to_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanLoyaltyPointIDR @Inject()(spark: SparkSession, statusManagerService: LPMutationStatusManager,
                                       LPMutationDf: LPMutationDf,
                                       ExchangeRateDf: LPMutationRateDf,
                                       GrandProductTypeDf: LPMutationGrandProductTypeDf,
                                       TransactionCategoryDf: LPMutationTransactionCategoryDf,
                                       UnderlyingProductDf: LPMutationUnderlyingProductDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

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
        $"lp_mutation.wallet_movement_id".as("wallet_movement_id"),
        $"lp_mutation.wallet_content_id".as("wallet_content_id"),
        $"lp_mutation.movement_type".as("movement_type"),
        $"lp_mutation.movement_time".as("movement_time"),
        $"lp_mutation.posting_date".as("posting_date"),
        $"lp_mutation.external_transaction_id".as("external_transaction_id"),
        $"lp_mutation.original_transaction_id".as("original_transaction_id"),
        $"lp_mutation.trip_type".as("trip_type"),
        $"lp_mutation.booking_product_type".as("booking_product_type"),
        $"mapping_underlying_product.underlying_product".as("mapping_underlying_product"),
        $"lp_mutation.transaction_type".as("transaction_type"),
        $"mapping_transaction_category.transaction_category".as("mapping_transaction_category"),
        $"lp_mutation.customer_id".as("customer_id"),
        $"lp_mutation.user_profile_id".as("user_profile_id"),
        $"lp_mutation.cost_type_id".as("cost_type_id"),
        $"mapping_grant_product_type.grant_product_type".as("mapping_grant_product_type"),
        $"lp_mutation.grant_product_type".as("grant_product_type"),
        $"lp_mutation.grant_entity".as("grant_entity"),
        $"lp_mutation.redeem_entity".as("redeem_entity"),
        $"lp_mutation.transaction_currency".as("transaction_currency"),
        $"lp_mutation.point_amount".as("point_amount"),
        when($"lp_mutation.transaction_currency" === "IDR",$"lp_mutation.point_amount")
          .otherwise($"lp_mutation.point_amount" * $"exchange_rate_idr.conversion_rate")
          .as("point_amount_idr"),
        $"lp_mutation.point_conversion_rate".as("point_conversion_rate"),
        $"lp_mutation.point_amount_in_transaction_currency".as("point_amount_in_transaction_currency"),
        when($"lp_mutation.transaction_currency" === "IDR",$"lp_mutation.point_amount_in_transaction_currency")
          .otherwise($"lp_mutation.point_amount_in_transaction_currency" * $"exchange_rate_idr.conversion_rate")
          .as("point_amount_in_transaction_currency_idr"),
        $"lp_mutation.selling_rate".as("selling_rate"),
        $"lp_mutation.selling_currency".as("selling_currency"),
        $"lp_mutation.earning_amount".as("earning_amount"),
        when($"lp_mutation.transaction_currency" === "IDR",$"lp_mutation.earning_amount")
          .otherwise($"lp_mutation.earning_amount" * $"exchange_rate_idr.conversion_rate")
          .as("earning_amount_idr"),
        $"lp_mutation.earning_currency".as("earning_currency"),
        $"lp_mutation.non_refundable_date".as("non_refundable_date"),
        $"lp_mutation.profit_center".as("profit_center"),
        $"lp_mutation.cost_center".as("cost_center"),
        $"lp_mutation.wc_original_transaction_id".as("wc_original_transaction_id"),
        $"lp_mutation.business_partner".as("business_partner"),
        $"lp_mutation.selling_conversion_rate".as("selling_conversion_rate"),
        $"lp_mutation.total_selling_price".as("total_selling_price"),
        $"lp_mutation.transaction_source".as("transaction_source"),
        $"lp_mutation.unit_id".as("unit_id"),
        $"lp_mutation.sales_issued_date".as("sales_issued_date"),
        $"lp_mutation.sales_redemption_amount".as("sales_redemption_amount"),
        $"lp_mutation.refunded_booking_id".as("refunded_booking_id"),
        $"lp_mutation.refund_id".as("refund_id"),
        $"lp_mutation.refund_provider_id".as("refund_provider_id"),
        $"lp_mutation.business_partner_id".as("business_partner_id"),
        $"lp_mutation.point_injection_id".as("point_injection_id"),
        $"lp_mutation.multiplier_booking_id".as("multiplier_booking_id"),
        $"lp_mutation.valid_from".as("valid_from"),
        $"lp_mutation.valid_until".as("valid_until"),
        $"lp_mutation.invoice_id".as("invoice_id"),
        $"lp_mutation.wc_transaction_source".as("wc_transaction_source"),
        $"lp_mutation.granted_point_wht_in_transaction_currency".as("granted_point_wht_in_transaction_currency"),
        when($"lp_mutation.transaction_currency" === "IDR",$"lp_mutation.granted_point_wht_in_transaction_currency")
          .otherwise($"lp_mutation.granted_point_wht_in_transaction_currency" * $"exchange_rate_idr.conversion_rate")
          .as("granted_point_wht_in_transaction_currency_idr"),
        $"lp_mutation.refund_issued_date".as("refund_issued_date"),
        $"lp_mutation.vat_selling_points".as("vat_selling_points"),
        $"lp_mutation.instant_active_point".as("instant_active_point")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
