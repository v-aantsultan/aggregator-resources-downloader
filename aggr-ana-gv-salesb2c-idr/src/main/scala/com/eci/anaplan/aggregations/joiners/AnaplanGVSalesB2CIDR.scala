package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors.{GVSalesB2CDf,GVB2CRateDf,GVB2CMDRDf}
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{coalesce, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanGVSalesB2CIDR @Inject()(spark: SparkSession,
                                     GVSalesB2CDf: GVSalesB2CDf,
                                     ExchangeRateDf: GVB2CRateDf,
                                     GVB2CMDRDf: GVB2CMDRDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

      GVSalesB2CDf.get.as("gv_sales_b2c")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"gv_sales_b2c.invoice_currency" === $"exchange_rate_idr.from_currency"
          && $"gv_sales_b2c.issued_date" === $"exchange_rate_idr.conversion_date"
        ,"left")
        .join(GVB2CMDRDf.get.as("mdr"),
          $"gv_sales_b2c.booking_id" === $"mdr.booking_id"
          ,"left")

        .withColumn("mdr_amount_prorate",
          (($"gv_sales_b2c.sum_invoice_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"gv_sales_b2c.count_bid"
        )

      .select(
        $"gv_sales_b2c.sales_delivery_id".as("sales_delivery_id"),
        $"gv_sales_b2c.entity".as("entity"),
        $"gv_sales_b2c.booking_id".as("booking_id"),
        $"gv_sales_b2c.product_type".as("product_type"),
        $"gv_sales_b2c.trip_type".as("trip_type"),
        $"gv_sales_b2c.invoice_amount".as("invoice_amount"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.invoice_amount")
          .otherwise($"gv_sales_b2c.invoice_amount" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("invoice_amount_idr"),
        $"gv_sales_b2c.invoice_currency".as("invoice_currency"),
        $"gv_sales_b2c.gift_voucher_amount".as("gift_voucher_amount"),
        $"gv_sales_b2c.gift_voucher_currency".as("gift_voucher_currency"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.gift_voucher_amount")
          .otherwise($"gv_sales_b2c.gift_voucher_amount" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("gift_voucher_amount_idr"),
        $"gv_sales_b2c.gift_voucher_id".as("gift_voucher_id"),
        $"gv_sales_b2c.issued_date".as("issued_date"),
        $"gv_sales_b2c.planned_delivery_date".as("planned_delivery_date"),
        $"gv_sales_b2c.unique_code".as("unique_code"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.unique_code")
          .otherwise($"gv_sales_b2c.unique_code" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("unique_code_idr"),
        $"gv_sales_b2c.coupon_value".as("coupon_value"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.coupon_value")
          .otherwise($"gv_sales_b2c.coupon_value" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("coupon_value_idr"),
        $"gv_sales_b2c.coupon_code".as("coupon_code"),
        $"gv_sales_b2c.discount_or_premium".as("discount_or_premium"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.discount_or_premium")
          .otherwise($"gv_sales_b2c.discount_or_premium" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("discount_or_premium_idr"),
        $"gv_sales_b2c.installment_fee_to_customer".as("installment_fee_to_customer"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.installment_fee_to_customer")
          .otherwise($"gv_sales_b2c.installment_fee_to_customer" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("installment_fee_to_customer_idr"),
        $"gv_sales_b2c.installment_code".as("installment_code"),
        $"gv_sales_b2c.payment_mdr_fee_to_channel".as("payment_mdr_fee_to_channel"),
        $"gv_sales_b2c.installment_mdr_fee_to_bank".as("installment_mdr_fee_to_bank"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.installment_mdr_fee_to_bank")
          .otherwise($"gv_sales_b2c.installment_mdr_fee_to_bank" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("installment_mdr_fee_to_bank_idr"),
        $"gv_sales_b2c.discount_wht".as("discount_wht"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.discount_wht")
          .otherwise($"gv_sales_b2c.discount_wht" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("discount_wht_idr"),
        $"gv_sales_b2c.coupon_wht".as("coupon_wht"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.coupon_wht")
          .otherwise($"gv_sales_b2c.coupon_wht" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("coupon_wht_idr"),
        $"gv_sales_b2c.payment_channel_name".as("payment_channel_name"),
        $"mdr_amount_prorate".as("mdr_amount_prorate"),
        coalesce(when($"gv_sales_b2c.invoice_currency" === "IDR",$"mdr_amount_prorate")
          .otherwise($"mdr_amount_prorate" * $"exchange_rate_idr.conversion_rate"),lit(0))
          .as("mdr_amount_idr")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
