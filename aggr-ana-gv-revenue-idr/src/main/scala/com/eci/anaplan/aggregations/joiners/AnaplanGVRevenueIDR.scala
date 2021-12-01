package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import com.eci.anaplan.services.StatusManagerService
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanGVRevenueIDR @Inject()(spark: SparkSession, statusManagerService: StatusManagerService,
                                    GVRevenueDf: GVRevenueDf,
                                    ExchangeRateDf: ExchangeRateDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    GVRevenueDf.get.as("gv_revenue")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"gv_revenue.revenue_currency" === $"exchange_rate_idr.from_currency"
          && $"gv_revenue.revenue_date" === to_date($"exchange_rate_idr.conversion_date")
        , "left")

      .select(
        $"gv_revenue.entity".as("entity"),
        $"gv_revenue.transaction_id".as("transaction_id"),
        $"gv_revenue.transaction_type".as("transaction_type"),
        $"gv_revenue.product_type".as("product_type"),
        $"gv_revenue.trip_type".as("trip_type"),
        $"gv_revenue.gift_voucher_id".as("gift_voucher_id"),
        $"gv_revenue.gift_voucher_currency".as("gift_voucher_currency"),
        $"gv_revenue.gift_voucher_amount".as("gift_voucher_amount"),
        $"gv_revenue.issued_date".as("issued_date"),
        $"gv_revenue.planned_delivery_date".as("planned_delivery_date"),
        $"gv_revenue.gift_voucher_expired_date".as("gift_voucher_expired_date"),
        $"gv_revenue.partner_name".as("partner_name"),
        $"gv_revenue.partner_id".as("partner_id"),
        $"gv_revenue.business_model".as("business_model"),
        when($"gv_revenue.revenue_currency" === "IDR",$"gv_revenue.gift_voucher_amount")
          .otherwise($"gv_revenue.gift_voucher_amount" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_amount_idr"),
        $"gv_revenue.redeemed_booking_id".as("redeemed_booking_id"),
        $"gv_revenue.redeemed_product_type".as("redeemed_product_type"),
        $"gv_revenue.redeemed_trip_type".as("redeemed_trip_type"),
        $"gv_revenue.redemption_date".as("redemption_date"),
        $"gv_revenue.used_amount".as("used_amount"),
        $"gv_revenue.revenue_amount".as("revenue_amount"),
        $"gv_revenue.revenue_currency".as("revenue_currency"),
        $"gv_revenue.revenue_date".as("revenue_date"),
        $"gv_revenue.status".as("status"),
        when($"gv_revenue.revenue_currency" === "IDR",$"gv_revenue.revenue_amount")
          .otherwise($"gv_revenue.revenue_amount" * $"exchange_rate_idr.conversion_rate")
          .as("revenue_amount_idr"),
        $"gv_revenue.selling_price".as("selling_price"),
        $"gv_revenue.discount_amount".as("discount_amount")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}