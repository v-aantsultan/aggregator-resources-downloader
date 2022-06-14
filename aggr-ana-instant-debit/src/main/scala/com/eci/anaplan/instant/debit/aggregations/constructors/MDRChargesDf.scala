package com.eci.anaplan.instant.debit.aggregations.constructors

import com.eci.anaplan.instant.debit.services.S3SourceService
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class MDRChargesDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    val InvoiceDf = s3SourceService.InvoiceSrc
      .select(
        $"`id`".as("id"),
        $"`booking_id`".as("booking_id"),
        $"`expected_amount`".as("expected_amount")
      )

    val PaymentDf = s3SourceService.PaymentSrc
      .select(
        $"`id`".as("id"),
        $"`invoice_id`".as("invoice_id")
      )

    val PaymentMdrDf = s3SourceService.PaymentMDRSrc
      .select(
        $"`payment_id`".as("payment_id"),
        $"`mdr_amount`".as("mdr_amount")
      )

    InvoiceDf.as("inv")
      .join(PaymentDf.as("pay"),
        $"inv.id" === $"pay.invoice_id",
        "left")
      .join(PaymentMdrDf.as("paymdr"),
        $"pay.id" === $"paymdr.payment_id",
        "left")

      .groupBy($"inv.booking_id")
      .agg(
        sum($"paymdr.mdr_amount").as("mdr_amount"),
        sum($"inv.expected_amount").as("expected_amount")
      )
      .select(
        $"inv.booking_id".as("booking_id"),
        $"mdr_amount",
        $"expected_amount"
      )
  }
}
