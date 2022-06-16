package com.eci.anaplan.ins.nonauto.aggregations.constructors

import com.eci.anaplan.ins.nonauto.services.S3SourceService
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class MDRChargesDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

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

  def getMDRCharges: DataFrame = {
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

    def getMDRInstallment: DataFrame = {
      val JoinedDf = InvoiceDf.as("inv")
        .join(PaymentDf.as("pay"),
          $"inv.id" === $"pay.invoice_id",
          "left")
        .join(PaymentMdrInstallmentDf.as("paymdr_ins"),
          $"pay.id" === $"paymdr_ins.payment_id",
          "left")

        .groupBy($"inv.booking_id", $"paymdr_ins.code_installment")
        .agg(
          sum($"paymdr_ins.mdr_installment_amount").as("mdr_installment_amount"),
          sum($"inv.expected_amount").as("expected_amount")
        )
        .select(
          $"inv.booking_id".as("booking_id"),
          $"paymdr_ins.code_installment".as("code_installment"),
          $"mdr_installment_amount",
          $"expected_amount"
        )

      JoinedDf.filter($"code_installment".isNotNull)
  }
}
