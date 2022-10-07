package com.eci.anaplan.ins.details.aggregations.constructors

import com.eci.anaplan.ins.details.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class CreditLifeInsuranceDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.CreditLifeInsuranceDf

      .select(
        to_date($"insurance_issued_date").as("report_date"),
        lit("ID").as("customer"),
        $"fulfillment_id".as("business_partner"),
        lit("IA").as("product"),
        $"product_name".as("product_category"),
        lit("None").as("payment_channel"),
        $"loan_id".as("loan_id"),
        $"insurance_premium".as("insurance_premium"),
        $"referral_fee_amount".as("referral_fee_amount"),
        lit(0).as("discount"),
        lit(0).as("premium"),
        lit(0).as("unique_code"),
        lit(0).as("coupon"),
        lit(0).as("mdr_charges")
      )
  }
}