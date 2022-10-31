package com.eci.anaplan.ic.paylater.r001.aggregations.constructors

import com.eci.common.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{exp, expr, lit, size, split, to_date, when}

import javax.inject.{Inject, Singleton}

@Singleton
class IcPaylaterR001DF @Inject() (
                                   val sparkSession: SparkSession,
                                   s3SourceService: S3SourceService
                                 ){
  import sparkSession.implicits._

  def getAllPeriod: DataFrame = {
      s3SourceService.IcPaylaterCsf01Src
        .select(
          to_date($"transaction_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
          $"channeling_agent_id".as("source_of_fund"),
          $"installment_plan".as("installment_plan"),
          $"loan_id".as("no_of_transactions"),
          $"transaction_amount".as("gmv"),
          $"commission_amount".as("admin_fee_commission"),
          $"interest_amount_after_subsidy".as("interest_amount"),
          // $"additional_interest_amount".as("additional_interest"),
          $"mdr_amount".as("mdr_fee"),
          $"user_acquisition_fee".as("service_income"),
          $"user_acquisition_fee".as("user_acquisition_fee")
        )
  }
}
