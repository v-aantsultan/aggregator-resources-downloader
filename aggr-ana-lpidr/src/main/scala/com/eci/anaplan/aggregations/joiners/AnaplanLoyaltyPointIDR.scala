package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import com.eci.anaplan.services.StatusManagerService
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{to_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * * Combine all the sub data sets and join them all together to produce anaplan CSV
 * * !!! STRICTLY DO NOT CHANGE THE ORDER OF THE SELECTED COLUMNS
 *
 * @param spark                       : Spark Session
 * @param statusManagerService        : Status manager service to manipulate status
 * @param LPMutationDf                : Test DataFrame1 Constructor
 */
@Singleton
class AnaplanLoyaltyPointIDR @Inject()(spark: SparkSession, statusManagerService: StatusManagerService,
                                       LPMutationDf: LPMutationDf,
                                       ExchangeRateDf: ExchangeRateDf,
                                       GrandProductTypeDf: GrandProductTypeDf,
                                       TransactionCategoryDf: TransactionCategoryDf,
                                       UnderlyingProductDf: UnderlyingProductDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    LPMutationDf.get.as("lp_mutation")
        // Join different data sets
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"lp_mutation.transaction_currency" === $"exchange_rate_idr.from_currency"
          && to_timestamp($"lp_mutation.posting_date") === $"exchange_rate_idr.conversion_date"
        , "left")
      .join(GrandProductTypeDf.get.as("mapping_grant_product_type"),
        $"lp_mutation.cost_type_id" === $"mapping_grant_product_type.map_cost_type_id"
        , "left")
      .join(TransactionCategoryDf.get.as("mapping_transaction_category"),
        $"lp_mutation.transaction_type" === $"mapping_transaction_category.map_transaction_type"
        , "left")
      .join(UnderlyingProductDf.get.as("mapping_underlying_product"),
        $"lp_mutation.booking_product_type" === $"mapping_underlying_product.map_booking_product_type"
        , "left")
      .as("lp_mutation_idr")

      .select(
        $"lp_mutation_idr.wallet_movement_id".as("wallet_movement_id"),
        $"lp_mutation_idr.wallet_content_id".as("wallet_content_id"),
        $"lp_mutation_idr.movement_type".as("movement_type"),
        $"lp_mutation_idr.movement_time".as("movement_time"),
        $"lp_mutation_idr.posting_date".as("posting_date"),
        $"lp_mutation_idr.external_transaction_id".as("external_transaction_id"),
        $"lp_mutation_idr.original_transaction_id".as("original_transaction_id"),
        $"lp_mutation_idr.trip_type".as("trip_type"),
        $"lp_mutation_idr.booking_product_type".as("booking_product_type"),
        $"lp_mutation_idr.map_underlying_product".as("mapping_underlying_product"),
        $"lp_mutation_idr.transaction_type".as("transaction_type"),
        $"lp_mutation_idr.map_transaction_category".as("mapping_transaction_category"),
        $"lp_mutation_idr.customer_id".as("customer_id"),
        $"lp_mutation_idr.user_profile_id".as("user_profile_id"),
        $"lp_mutation_idr.cost_type_id".as("cost_type_id"),
        $"lp_mutation_idr.map_grant_product_type".as("mapping_grant_product_type"),
        $"lp_mutation_idr.grant_product_type".as("grant_product_type"),
        $"lp_mutation_idr.grant_entity".as("grant_entity"),
        $"lp_mutation_idr.redeem_entity".as("redeem_entity"),
        $"lp_mutation_idr.transaction_currency".as("transaction_currency"),
        $"lp_mutation_idr.point_amount".as("point_amount"),
        when($"lp_mutation_idr.transaction_currency" === "IDR",$"lp_mutation_idr.point_amount")
          .otherwise($"lp_mutation_idr.point_amount" * $"lp_mutation_idr.conversion_rate")
          .as("point_amount_idr"),
        $"lp_mutation_idr.point_conversion_rate".as("point_conversion_rate"),
        $"lp_mutation_idr.point_amount_in_transaction_currency".as("point_amount_in_transaction_currency"),
        when($"lp_mutation_idr.transaction_currency" === "IDR",$"lp_mutation_idr.point_amount_in_transaction_currency")
          .otherwise($"lp_mutation_idr.point_amount_in_transaction_currency" * $"lp_mutation_idr.conversion_rate")
          .as("point_amount_in_transaction_currency_idr"),
        $"lp_mutation_idr.selling_rate".as("selling_rate"),
        $"lp_mutation_idr.selling_currency".as("selling_currency"),
        $"lp_mutation_idr.earning_amount".as("earning_amount"),
        when($"lp_mutation_idr.transaction_currency" === "IDR",$"lp_mutation_idr.earning_amount")
          .otherwise($"lp_mutation_idr.earning_amount" * $"lp_mutation_idr.conversion_rate")
          .as("earning_amount_idr"),
        $"lp_mutation_idr.earning_currency".as("earning_currency"),
        $"lp_mutation_idr.non_refundable_date".as("non_refundable_date"),
        $"lp_mutation_idr.profit_center".as("profit_center"),
        $"lp_mutation_idr.cost_center".as("cost_center"),
        $"lp_mutation_idr.wc_original_transaction_id".as("wc_original_transaction_id"),
        $"lp_mutation_idr.business_partner".as("business_partner"),
        $"lp_mutation_idr.selling_conversion_rate".as("selling_conversion_rate"),
        $"lp_mutation_idr.total_selling_price".as("total_selling_price"),
        $"lp_mutation_idr.transaction_source".as("transaction_source"),
        $"lp_mutation_idr.unit_id".as("unit_id"),
        $"lp_mutation_idr.sales_issued_date".as("sales_issued_date"),
        $"lp_mutation_idr.sales_redemption_amount".as("sales_redemption_amount"),
        $"lp_mutation_idr.refunded_booking_id".as("refunded_booking_id"),
        $"lp_mutation_idr.refund_id".as("refund_id"),
        $"lp_mutation_idr.refund_provider_id".as("refund_provider_id"),
        $"lp_mutation_idr.business_partner_id".as("business_partner_id"),
        $"lp_mutation_idr.point_injection_id".as("point_injection_id"),
        $"lp_mutation_idr.multiplier_booking_id".as("multiplier_booking_id"),
        $"lp_mutation_idr.valid_from".as("valid_from"),
        $"lp_mutation_idr.valid_until".as("valid_until"),
        $"lp_mutation_idr.invoice_id".as("invoice_id"),
        $"lp_mutation_idr.wc_transaction_source".as("wc_transaction_source"),
        $"lp_mutation_idr.granted_point_wht_in_transaction_currency".as("granted_point_wht_in_transaction_currency"),
        when($"lp_mutation_idr.transaction_currency" === "IDR",$"lp_mutation_idr.granted_point_wht_in_transaction_currency")
          .otherwise($"lp_mutation_idr.granted_point_wht_in_transaction_currency" * $"lp_mutation_idr.conversion_rate")
          .as("granted_point_wht_in_transaction_currency_idr"),
        $"lp_mutation_idr.refund_issued_date".as("refund_issued_date"),
        $"lp_mutation_idr.vat_selling_points".as("vat_selling_points"),
        $"lp_mutation_idr.instant_active_point".as("instant_active_point")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
