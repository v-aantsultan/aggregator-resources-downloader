package com.eci.anaplan.ua.details.aggregations.constructors

import com.eci.anaplan.ua.details.services.S3SourceService
import org.apache.spark.sql.functions.{coalesce, lit, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class UADetailsDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = s3SourceService.UADetailsDf

    .select(
      to_date($"mutation_date_yyyy-mm-dd").as("report_date"),
      coalesce(
        when($"travelokapay_order_type" === "TOP_UP" || $"travelokapay_order_type" === "BALANCE_EXCESS", "Topup"),
        when($"travelokapay_order_type" === "REFUND_TO_WALLET", "Refund"),
        when($"travelokapay_order_type" === "BALANCE_CASHBACK", "Cashback Mutation"),
        when($"travelokapay_order_type" === "PAYMENT" ||
          $"travelokapay_order_type" === "EXTERNAL_MUTATION" && $"uangku_transaction_type" === "PURCHASE" ||
          $"travelokapay_order_type" === "EXTERNAL_MUTATION" && $"uangku_transaction_type" === "PAYMENT", "Purchase"),
        when($"travelokapay_order_type" === "EXTERNAL_MUTATION" &&
          $"uangku_transaction_type" === "AGENT_CASHOUT" ||
          $"travelokapay_order_type" === "EXTERNAL_MUTATION" && $"uangku_transaction_type" === "WALLET" &&
            !$"external_request_id".like("%-%"), "Withdrawal"),
        when($"travelokapay_order_type" === "EXTERNAL_MUTATION" &&
          $"uangku_transaction_type" === "AGENT_CASHOUT" && $"external_request_id".like("%R%") ||
          $"travelokapay_order_type" === "EXTERNAL_MUTATION" && $"uangku_transaction_type" === "WALLET" &&
            $"external_request_id".like("%REVERSAL%"), "Loan Reversal"),
        when($"travelokapay_order_type" === "EXTERNAL_MUTATION"  && $"uangku_transaction_type" === "WALLET" &&
          $"external_request_id".like("%S%"), "P2P-S"),
        when($"travelokapay_order_type" === "EXTERNAL_MUTATION"  && $"uangku_transaction_type" === "WALLET" &&
          $"external_request_id".like("%D%"), "P2P-D")
        otherwise $"travelokapay_order_type",lit(""))
        .as("type_of_transaction"),
      $"mutation_amount".as("mutation_amount"),
      $"mutation_id".as("mutation_id")
    )
}