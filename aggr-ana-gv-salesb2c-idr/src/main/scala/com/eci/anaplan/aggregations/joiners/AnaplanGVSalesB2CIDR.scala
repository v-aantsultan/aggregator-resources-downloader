package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import com.eci.anaplan.services.StatusManagerService
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanGVSalesB2CIDR @Inject()(spark: SparkSession, statusManagerService: StatusManagerService,
                                     GVSalesB2CDf: GVSalesB2CDf,
                                     ExchangeRateDf: ExchangeRateDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

      GVSalesB2CDf.get.as("gv_sales_b2c")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"gv_sales_b2c.invoice_currency" === $"exchange_rate_idr.from_currency"
          && $"gv_sales_b2c.issued_date" === to_date($"exchange_rate_idr.conversion_date")
        , "left")

      .select(
        $"gv_sales_b2c.sales_delivery_id".as("sales_delivery_id"),
        $"gv_sales_b2c.entity".as("entity"),
        $"gv_sales_b2c.booking_id".as("booking_id"),
        $"gv_sales_b2c.product_type".as("product_type"),
        $"gv_sales_b2c.trip_type".as("trip_type"),
        $"gv_sales_b2c.invoice_amount".as("invoice_amount"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.invoice_amount")
          .otherwise($"gv_sales_b2c.invoice_amount" * $"exchange_rate_idr.conversion_rate")
          .as("invoice_amount_idr"),
        $"gv_sales_b2c.invoice_currency".as("invoice_currency"),
        $"gv_sales_b2c.gift_voucher_amount".as("gift_voucher_amount"),
        $"gv_sales_b2c.gift_voucher_currency".as("gift_voucher_currency"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.gift_voucher_amount")
          .otherwise($"gv_sales_b2c.gift_voucher_amount" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_amount_idr"),
        $"gv_sales_b2c.gift_voucher_id".as("gift_voucher_id"),
        $"gv_sales_b2c.issued_date".as("issued_date"),
        $"gv_sales_b2c.planned_delivery_date".as("planned_delivery_date"),
        $"gv_sales_b2c.unique_code".as("unique_code"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.unique_code")
          .otherwise($"gv_sales_b2c.unique_code" * $"exchange_rate_idr.conversion_rate")
          .as("unique_code_idr"),
        $"gv_sales_b2c.coupon_value".as("coupon_value"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.coupon_value")
          .otherwise($"gv_sales_b2c.coupon_value" * $"exchange_rate_idr.conversion_rate")
          .as("coupon_value_idr"),
        $"gv_sales_b2c.coupon_code".as("coupon_code"),
        $"gv_sales_b2c.discount_or_premium".as("discount_or_premium"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.discount_or_premium")
          .otherwise($"gv_sales_b2c.discount_or_premium" * $"exchange_rate_idr.conversion_rate")
          .as("discount_or_premium_idr"),
        $"gv_sales_b2c.installment_fee_to_customer".as("installment_fee_to_customer"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.installment_fee_to_customer")
          .otherwise($"gv_sales_b2c.installment_fee_to_customer" * $"exchange_rate_idr.conversion_rate")
          .as("installment_fee_to_customer_idr"),
        $"gv_sales_b2c.installment_code".as("installment_code"),
        $"gv_sales_b2c.payment_mdr_fee_to_channel".as("payment_mdr_fee_to_channel"),
        $"gv_sales_b2c.installment_mdr_fee_to_bank".as("installment_mdr_fee_to_bank"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.installment_mdr_fee_to_bank")
          .otherwise($"gv_sales_b2c.installment_mdr_fee_to_bank" * $"exchange_rate_idr.conversion_rate")
          .as("installment_mdr_fee_to_bank_idr"),
        $"gv_sales_b2c.discount_wht".as("discount_wht"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.discount_wht")
          .otherwise($"gv_sales_b2c.discount_wht" * $"exchange_rate_idr.conversion_rate")
          .as("discount_wht_idr"),
        $"gv_sales_b2c.coupon_wht".as("coupon_wht"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.coupon_wht")
          .otherwise($"gv_sales_b2c.coupon_wht" * $"exchange_rate_idr.conversion_rate")
          .as("coupon_wht_idr"),
        $"gv_sales_b2c.payment_channel_name".as("payment_channel_name")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
