package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import com.eci.anaplan.services.StatusManagerService
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanGVSalesB2BIDR @Inject()(spark: SparkSession, statusManagerService: StatusManagerService,
                                     GVSalesB2BDf: GVSalesB2BDf,
                                     ExchangeRateDf: ExchangeRateDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    GVSalesB2BDf.get.as("gv_sales_b2b")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"gv_sales_b2b.gift_voucher_currency" === $"exchange_rate_idr.from_currency"
          && $"gv_sales_b2b.report_date" === to_date($"exchange_rate_idr.conversion_date")
        ,"left")

      .select(
        $"gv_sales_b2b.movement_id".as("movement_id"),
        $"gv_sales_b2b.contract_id".as("contract_id"),
        $"gv_sales_b2b.partner_id".as("partner_id"),
        $"gv_sales_b2b.movement_type".as("movement_type"),
        $"gv_sales_b2b.movement_time".as("movement_time"),
        $"gv_sales_b2b.movement_status".as("movement_status"),
        $"gv_sales_b2b.unit_amount".as("unit_amount"),
        $"gv_sales_b2b.unit_id".as("unit_id"),
        $"gv_sales_b2b.processed_amount".as("processed_amount"),
        $"gv_sales_b2b.gift_voucher_id".as("gift_voucher_id"),
        $"gv_sales_b2b.gift_voucher_valid_from".as("gift_voucher_valid_from"),
        $"gv_sales_b2b.gift_voucher_valid_until".as("gift_voucher_valid_until"),
        $"gv_sales_b2b.gift_voucher_nominal".as("gift_voucher_nominal"),
        $"gv_sales_b2b.gift_voucher_currency".as("gift_voucher_currency"),
        when($"gv_sales_b2b.gift_voucher_currency" === "IDR",$"gv_sales_b2b.gift_voucher_nominal")
          .otherwise($"gv_sales_b2b.gift_voucher_nominal" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_nominal_idr"),
        $"gv_sales_b2b.profit_center".as("profit_center"),
        $"gv_sales_b2b.cost_center".as("cost_center"),
        $"gv_sales_b2b.contract_valid_from".as("contract_valid_from"),
        $"gv_sales_b2b.contract_valid_until".as("contract_valid_until"),
        $"gv_sales_b2b.selling_price".as("selling_price"),
        $"gv_sales_b2b.collecting_entity".as("collecting_entity"),
        $"gv_sales_b2b.contracting_entity".as("contracting_entity"),
        $"gv_sales_b2b.business_model".as("business_model"),
        $"gv_sales_b2b.contract_name".as("contract_name"),
        $"gv_sales_b2b.contract_number".as("contract_number"),
        $"gv_sales_b2b.partner_name".as("partner_name"),
        $"gv_sales_b2b.transaction_source_id".as("transaction_source_id"),
        $"gv_sales_b2b.deduction_type".as("deduction_type"),
        $"gv_sales_b2b.discount_percentage".as("discount_percentage"),
        $"gv_sales_b2b.discount_amount".as("discount_amount"),
        when($"gv_sales_b2b.gift_voucher_currency" === "IDR",$"gv_sales_b2b.discount_amount")
          .otherwise($"gv_sales_b2b.discount_amount" * $"exchange_rate_idr.conversion_rate")
          .as("discount_amount_idr"),
        $"gv_sales_b2b.gift_voucher_net_amount".as("gift_voucher_net_amount"),
        when($"gv_sales_b2b.gift_voucher_currency" === "IDR",$"gv_sales_b2b.gift_voucher_net_amount")
          .otherwise($"gv_sales_b2b.gift_voucher_net_amount" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_net_amount_idr"),
        $"gv_sales_b2b.deposit_liabilities_amount".as("deposit_liabilities_amount"),
        $"gv_sales_b2b.deposit_liabilities_currency".as("deposit_liabilities_currency"),
        $"gv_sales_b2b.report_date".as("report_date"),
        $"gv_sales_b2b.discount_wht".as("discount_wht"),
        when($"gv_sales_b2b.gift_voucher_currency" === "IDR",$"gv_sales_b2b.discount_wht")
          .otherwise($"gv_sales_b2b.discount_wht" * $"exchange_rate_idr.conversion_rate")
          .as("discount_wht_idr")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}