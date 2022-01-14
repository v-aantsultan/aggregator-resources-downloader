package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVIssuedSource
import org.apache.spark.sql.functions.{expr, to_date}
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class GVIssuedlistDf @Inject()(val sparkSession: SparkSession, s3SourceService: GVIssuedSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.GVIssuedlistDf
      .select(
        $"`entity`".as("entity"),
        $"`transaction_id`".as("transaction_id"),
        $"`transaction_type`".as("transaction_type"),
        $"`product_type`".as("product_type"),
        $"`trip_type`".as("trip_type"),
        $"`gift_voucher_id`".as("gift_voucher_id"),
        $"`gift_voucher_currency`".as("gift_voucher_currency"),
        $"`gift_voucher_amount`".as("gift_voucher_amount"),
        to_date($"`issued_date`" + expr("INTERVAL 7 HOURS")).as("issued_date"),
        $"`planned_delivery_date`".as("planned_delivery_date"),
        $"`gift_voucher_expired_date`".as("gift_voucher_expired_date"),
        $"`partner_name`".as("partner_name"),
        $"`partner_id`".as("partner_id"),
        $"`business_model`".as("business_model"),
        $"`contract_valid_from`".as("contract_valid_from"),
        $"`contract_valid_until`".as("contract_valid_until")
      )
  }
}
