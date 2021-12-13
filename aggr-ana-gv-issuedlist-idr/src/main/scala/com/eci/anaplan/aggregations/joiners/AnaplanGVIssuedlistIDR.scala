package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{coalesce, lit, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanGVIssuedlistIDR @Inject()(spark: SparkSession,
                                       GVIssuedistDf: GVIssuedlistDf,
                                       ExchangeRateDf: GVIssuedRateDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

      GVIssuedistDf.get.as("gv_issuedlist")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"gv_issuedlist.gift_voucher_currency" === $"exchange_rate_idr.from_currency"
          && $"gv_issuedlist.issued_date" === to_date($"exchange_rate_idr.conversion_date")
        , "left")

      .select(
        $"gv_issuedlist.entity".as("entity"),
        $"gv_issuedlist.transaction_id".as("transaction_id"),
        $"gv_issuedlist.transaction_type".as("transaction_type"),
        $"gv_issuedlist.product_type".as("product_type"),
        $"gv_issuedlist.trip_type".as("trip_type"),
        $"gv_issuedlist.gift_voucher_id".as("gift_voucher_id"),
        $"gv_issuedlist.gift_voucher_currency".as("gift_voucher_currency"),
        $"gv_issuedlist.gift_voucher_amount".as("gift_voucher_amount"),
        $"gv_issuedlist.issued_date" .as("issued_date"),
        $"gv_issuedlist.planned_delivery_date".as("planned_delivery_date"),
        $"gv_issuedlist.gift_voucher_expired_date".as("gift_voucher_expired_date"),
        $"gv_issuedlist.partner_name".as("partner_name"),
        $"gv_issuedlist.partner_id".as("partner_id"),
        $"gv_issuedlist.business_model".as("business_model"),
        coalesce(when($"gv_issuedlist.gift_voucher_currency" === "IDR",$"gv_issuedlist.gift_voucher_amount")
          .otherwise($"gv_issuedlist.gift_voucher_amount" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("gift_voucher_amount_idr"),
        $"gv_issuedlist.contract_valid_from".as("contract_valid_from"),
        $"gv_issuedlist.contract_valid_until".as("contract_valid_until")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
