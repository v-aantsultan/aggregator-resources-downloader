package com.eci.anaplan.instant.debit.aggregations.constructors

import com.eci.anaplan.instant.debit.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class PaymentScopeSheetDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.PaymentScopeSheetSrc
      .select(
        $"list_of_instant_debit_payment_scope".as("instant_debit_payment_scope"),
        $"business_partner".as("business_partner")
      )
  }
}
