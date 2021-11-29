package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.S3SourceService
import org.apache.spark.sql.functions.{expr, to_date}
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class GVSalesB2BDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.GVSalesB2BDf
      .select(
        $"`movement_id`".as("movement_id"),
        $"`contract_id`".as("contract_id"),
        $"`partner_id`".as("partner_id"),
        $"`movement_type`".as("movement_type"),
        $"`movement_time`".as("movement_time"),
        $"`movement_status`".as("movement_status"),
        $"`unit_amount`".as("unit_amount"),
        $"`unit_id`".as("unit_id"),
        $"`processed_amount`".as("processed_amount"),
        $"`gift_voucher_id`".as("gift_voucher_id"),
        $"`gift_voucher_valid_from`".as("gift_voucher_valid_from"),
        $"`gift_voucher_valid_until`".as("gift_voucher_valid_until"),
        $"`gift_voucher_nominal`".as("gift_voucher_nominal"),
        $"`gift_voucher_currency`".as("gift_voucher_currency"),
        $"`profit_center`".as("profit_center"),
        $"`cost_center`".as("cost_center"),
        $"`contract_valid_from`".as("contract_valid_from"),
        $"`contract_valid_until`".as("contract_valid_until"),
        $"`selling_price`".as("selling_price"),
        $"`collecting_entity`".as("collecting_entity"),
        $"`contracting_entity`".as("contracting_entity"),
        $"`business_model`".as("business_model"),
        $"`contract_name`".as("contract_name"),
        $"`contract_number`".as("contract_number"),
        $"`partner_name`".as("partner_name"),
        $"`transaction_source_id`".as("transaction_source_id"),
        $"`deduction_type`".as("deduction_type"),
        $"`discount_percentage`".as("discount_percentage"),
        $"`discount_amount`".as("discount_amount"),
        $"`gift_voucher_net_amount`".as("gift_voucher_net_amount"),
        $"`deposit_liabilities_amount`".as("deposit_liabilities_amount"),
        $"`deposit_liabilities_currency`".as("deposit_liabilities_currency"),
        to_date($"`report_date`" + expr("INTERVAL 7 HOURS")).as("report_date"),
        $"`discount_wht`".as("discount_wht")
      )
  }
}
