package com.eci.anaplan.ins.nonauto.aggregations.constructors

import com.eci.anaplan.ins.nonauto.services.S3SourceService
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class MDRChargesDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    val InvoiceDf = s3SourceService.InvoiceDf
      .select(
        $"`id`".as("id"),
        $"`booking_id`".as("booking_id"),
        $"`expected_amount`".as("expected_amount")
      )

    val PaymentDf = s3SourceService.PaymentDf
      .select(
        $"`id`".as("id"),
        $"`invoice_id`".as("invoice_id")
      )

    val PaymentMdrDf = s3SourceService.PaymentMDRDf
      .select(
        $"`payment_id`".as("payment_id"),
        $"`mdr_amount`".as("mdr_amount")
      )

    val PaymentMdrInstallmentDf = s3SourceService.PaymentMDRInstallmentDf
      .select(
        $"`payment_id`".as("payment_id"),
        $"`code`".as("code_installment"),
        $"`amount`".as("mdr_installment_amount")
      )

    val filterMDRInstallment = PaymentMdrInstallmentDf.filter($"code_installment".isNotNull)

    InvoiceDf.as("inv")
      .join(PaymentDf.as("pay"),
        $"inv.id" === $"pay.invoice_id",
        "left")
      .join(PaymentMdrDf.as("paymdr"),
        $"pay.id" === $"paymdr.payment_id",
        "left")
      .join(filterMDRInstallment.as("paymdr_ins"),
        $"pay.id" === $"paymdr_ins.payment_id",
        "left")

      .groupBy($"inv.booking_id", $"paymdr_ins.code_installment")
      .agg(
        sum($"paymdr.mdr_amount").as("mdr_amount"),
        sum($"paymdr_ins.mdr_installment_amount").as("mdr_installment_amount"),
        sum($"inv.expected_amount").as("expected_amount")
      )
      .select(
        $"inv.booking_id".as("booking_id"),
        $"paymdr_ins.code_installment".as("code_installment"),
        $"mdr_amount",
        $"mdr_installment_amount",
        $"expected_amount"
      )
  }
}
