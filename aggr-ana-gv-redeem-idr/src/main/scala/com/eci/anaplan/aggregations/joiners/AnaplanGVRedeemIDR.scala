package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import com.eci.anaplan.services.StatusManagerService
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanGVRedeemIDR @Inject()(spark: SparkSession, statusManagerService: StatusManagerService,
                                   GVRedeemDf: GVRedeemDf,
                                   ExchangeRateDf: ExchangeRateDf,
                                   UnderlyingProductDf: UnderlyingProductDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    GVRedeemDf.get.as("gv_redeem")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"gv_redeem.gift_voucher_currency" === $"exchange_rate_idr.from_currency"
          && $"gv_redeem.redemption_date" === to_date($"exchange_rate_idr.conversion_date")
        , "left")
      .join(UnderlyingProductDf.get.as("underlying_product"),
        $"gv_redeem.redeemed_product_type" === $"underlying_product.fs_product_type"
        , "left")

      .select(
        $"gv_redeem.entity".as("entity"),
        $"gv_redeem.transaction_id".as("transaction_id"),
        $"gv_redeem.transaction_type".as("transaction_type"),
        $"gv_redeem.product_type".as("product_type"),
        $"gv_redeem.trip_type".as("trip_type"),
        $"gv_redeem.gift_voucher_id".as("gift_voucher_id"),
        $"gv_redeem.gift_voucher_currency".as("gift_voucher_currency"),
        $"gv_redeem.gift_voucher_amount".as("gift_voucher_amount"),
        $"gv_redeem.issued_date".as("issued_date"),
        $"gv_redeem.planned_delivery_date".as("planned_delivery_date"),
        $"gv_redeem.gift_voucher_expired_date".as("gift_voucher_expired_date"),
        $"gv_redeem.partner_name".as("partner_name"),
        $"gv_redeem.partner_id".as("partner_id"),
        $"gv_redeem.business_model".as("business_model"),
        when($"gv_redeem.gift_voucher_currency" === "IDR",$"gv_redeem.gift_voucher_amount")
          .otherwise($"gv_redeem.gift_voucher_amount" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_amount_idr"),
        $"gv_redeem.redeemed_booking_id".as("redeemed_booking_id"),
        $"gv_redeem.redeemed_product_type".as("redeemed_product_type"),
        $"underlying_product.underlying_product".as("underlying_product"),
        $"gv_redeem.redeemed_trip_type".as("redeemed_trip_type"),
        $"gv_redeem.redemption_date_ori".as("redemption_date_ori"),
        $"gv_redeem.redemption_date".as("redemption_date"),
        $"gv_redeem.gift_voucher_redeemed_amount".as("gift_voucher_redeemed_amount"),
        when($"gv_redeem.gift_voucher_currency" === "IDR",$"gv_redeem.gift_voucher_redeemed_amount")
          .otherwise($"gv_redeem.gift_voucher_redeemed_amount" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_redeemed_amount_idr"),
        $"gv_redeem.selling_price".as("selling_price"),
        $"gv_redeem.discount_amount".as("discount_amount")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
