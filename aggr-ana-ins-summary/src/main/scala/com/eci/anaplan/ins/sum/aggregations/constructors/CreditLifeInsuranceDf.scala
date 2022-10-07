package com.eci.anaplan.ins.sum.aggregations.constructors

import com.eci.anaplan.ins.sum.services.S3SourceService
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
        lit("IA").as("product"),
        lit("None").as("payment_channel"),
        $"loan_id".as("loan_id")
      )
  }
}